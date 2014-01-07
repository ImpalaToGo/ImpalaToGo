/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.llama.am.impl;

import com.cloudera.llama.am.api.LlamaAM;
import com.cloudera.llama.am.api.LlamaAMEvent;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.am.api.LlamaAMListener;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.server.MetricUtil;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class GangAntiDeadlockLlamaAM extends LlamaAMImpl implements
    LlamaAMListener, Runnable {
  private static final Logger LOG = 
      LoggerFactory.getLogger(GangAntiDeadlockLlamaAM.class);

  private static final String METRIC_PREFIX = LlamaAM.METRIC_PREFIX +
      "gang-anti-deadlock.";

  private static final String BACKED_OFF_RESERVATIONS_METER = METRIC_PREFIX +
      "backed-off-reservations.meter";
  private static final String BACKED_OFF_RESOURCES_METER = METRIC_PREFIX +
      "backed-off-resources.meter";

  public static final List<String> METRIC_KEYS = Arrays.asList(
      BACKED_OFF_RESERVATIONS_METER, BACKED_OFF_RESOURCES_METER);

  static class BackedOffReservation implements Delayed {
    private PlacedReservationImpl reservation;
    private long delayedUntil;

    public BackedOffReservation(PlacedReservationImpl reservation, long delay) {
      this.reservation = reservation;
      this.delayedUntil = System.currentTimeMillis() + delay;
    }

    public PlacedReservationImpl getReservation() {
      return reservation;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(delayedUntil - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      return (int) (delayedUntil - ((BackedOffReservation) o).delayedUntil);
    }
  }

  private final LlamaAM am;

  //visible for testing
  Map<UUID, PlacedReservationImpl> localReservations;
  Set<UUID> submittedReservations;
  DelayQueue<BackedOffReservation> backedOffReservations;
  volatile long timeOfLastAllocation;

  private long noAllocationLimit;
  private int backOffPercent;
  private long backOffMinDelay;
  private long backOffMaxDelay;
  private Random random;

  public GangAntiDeadlockLlamaAM(Configuration conf, LlamaAM llamaAM) {
    super(conf);
    this.am = llamaAM;
    am.addListener(this);
  }

  @Override
  public void setMetricRegistry(MetricRegistry metricRegistry) {
    super.setMetricRegistry(metricRegistry);
    am.setMetricRegistry(metricRegistry);
    if (metricRegistry != null) {
      MetricUtil.registerMeter(metricRegistry, BACKED_OFF_RESERVATIONS_METER);
      MetricUtil.registerMeter(metricRegistry, BACKED_OFF_RESOURCES_METER);
    }
  }

  @Override
  public void start() throws LlamaException {
    am.start();
    localReservations = new HashMap<UUID, PlacedReservationImpl>();
    submittedReservations = new HashSet<UUID>();
    backedOffReservations = new DelayQueue<BackedOffReservation>();
    noAllocationLimit = getConf().getLong(
        GANG_ANTI_DEADLOCK_NO_ALLOCATION_LIMIT_KEY,
        GANG_ANTI_DEADLOCK_NO_ALLOCATION_LIMIT_DEFAULT);
    backOffPercent = getConf().getInt(
        GANG_ANTI_DEADLOCK_BACKOFF_PERCENT_KEY,
        GANG_ANTI_DEADLOCK_BACKOFF_PERCENT_DEFAULT);
    backOffMinDelay = getConf().getLong(
        GANG_ANTI_DEADLOCK_BACKOFF_MIN_DELAY_KEY,
        GANG_ANTI_DEADLOCK_BACKOFF_MIN_DELAY_DEFAULT);
    backOffMaxDelay = getConf().getLong(
        GANG_ANTI_DEADLOCK_BACKOFF_MAX_DELAY_KEY,
        GANG_ANTI_DEADLOCK_BACKOFF_MAX_DELAY_DEFAULT);
    random = new Random();
    timeOfLastAllocation = System.currentTimeMillis();
    startDeadlockResolverThread();
    am.addListener(this);
    LOG.info("Gang scheduling anti-deadlock enabled, no allocation " +
        "limit '{}' ms, resources backoff '{}' %", noAllocationLimit,
        backOffPercent);
  }

  //visible for testing
  void startDeadlockResolverThread() {
    Thread deadlockResolverThread = new Thread(this);
    deadlockResolverThread.setDaemon(true);
    deadlockResolverThread.setName("llama-gang-antideadlock");
    deadlockResolverThread.start();
  }

  @Override
  public void stop() {
    am.stop();
  }

  @Override
  public boolean isRunning() {
    return am.isRunning();
  }

  @Override
  public List<String> getNodes() throws LlamaException {
    return am.getNodes();
  }

  @Override
  public void reserve(UUID reservationId, Reservation reservation)
      throws LlamaException {
    boolean doActualReservation = true;
    if (reservation.isGang()) {
      PlacedReservationImpl placedReservation =
          new PlacedReservationImpl(reservationId, reservation);
      doActualReservation = gReserve(reservationId, placedReservation);
      if (!doActualReservation) {
        dispatch(LlamaAMEventImpl.createEvent(true, placedReservation));
      }
    }
    if (doActualReservation) {
      am.reserve(reservationId, reservation);
    }
  }

  private synchronized boolean gReserve(UUID reservationId,
      PlacedReservationImpl placedReservation) {
    boolean doActualReservation;
    localReservations.put(reservationId, placedReservation);
    if (backedOffReservations.isEmpty()) {
      submittedReservations.add(reservationId);
      doActualReservation = true;
    } else {
      placedReservation.setStatus(PlacedReservation.Status.BACKED_OFF);
      long delay = getBackOffDelay();
      backedOffReservations.add(new BackedOffReservation(placedReservation,
          delay));
      LOG.warn(
          "Back off in effect, delaying placing reservation '{}' for '{}' ms",
          reservationId, delay);
      doActualReservation = false;
    }
    return doActualReservation;
  }

  @Override
  public PlacedReservation getReservation(UUID reservationId)
      throws LlamaException {
    PlacedReservation pr = am.getReservation(reservationId);
    if (pr == null) {
      pr = gGetReservation(reservationId);
    }
    return pr;
  }

  private synchronized PlacedReservation gGetReservation(UUID reservationId) {
    return localReservations.get(reservationId);
  }

  @Override
  public PlacedReservation releaseReservation(UUID handle, UUID reservationId,
      boolean doNotCache) throws LlamaException {
    PlacedReservation gPlacedReservation = gReleaseReservation(reservationId);
    PlacedReservation placedReservation = am.releaseReservation(handle,
        reservationId, doNotCache);
    if (gPlacedReservation != null && placedReservation == null) {
      //reservation was local only
      dispatch(LlamaAMEventImpl.createEvent(isCallConsideredEcho(handle),
          gPlacedReservation));
    }
    return (placedReservation != null) ? placedReservation : gPlacedReservation;
  }

  private synchronized PlacedReservation gReleaseReservation(UUID reservationId) {
    PlacedReservationImpl pr = localReservations.remove(reservationId);
    if (pr != null) {
      pr.setStatus(PlacedReservation.Status.RELEASED);
    }
    submittedReservations.remove(reservationId);
    return pr;
  }

  @Override
  public List<PlacedReservation> releaseReservationsForHandle(UUID handle,
      boolean doNotCache)
      throws LlamaException {
    List<PlacedReservation> reservations =
        am.releaseReservationsForHandle(handle, doNotCache);
    List<PlacedReservation> localReservations =
        gReleaseReservationsForHandle(handle);
    localReservations.removeAll(reservations);
    if (!localReservations.isEmpty()) {
      dispatch(LlamaAMEventImpl.createEvent(isCallConsideredEcho(handle),
          localReservations));
    }
    reservations = new ArrayList<PlacedReservation>(reservations);
    reservations.addAll(localReservations);
    return reservations;
  }

  private synchronized List<PlacedReservation> gReleaseReservationsForHandle(
      UUID handle) {
    List<PlacedReservation> reservations = new ArrayList<PlacedReservation>();
    Iterator<PlacedReservationImpl> it =
        localReservations.values().iterator();
    while (it.hasNext()) {
      PlacedReservationImpl pr = it.next();
      if (pr.getHandle().equals(handle)) {
        it.remove();
        submittedReservations.remove(pr.getReservationId());
        reservations.add(pr);
        pr.setStatus(PlacedReservation.Status.RELEASED);
        LOG.debug(
            "Releasing all reservations for handle '{}', reservationId '{}'",
            handle, pr.getReservationId());
      }
    }
    return reservations;
  }

  public List<PlacedReservation> releaseReservationsForQueue(
      String queue, boolean doNotCache) throws LlamaException {
    List<PlacedReservation> reservations =
        am.releaseReservationsForQueue(queue, doNotCache);

    List<PlacedReservation> localReservations =
        gReleaseReservationsForQueue(queue);
    localReservations.removeAll(reservations);
    if (!localReservations.isEmpty()) {
      dispatch(LlamaAMEventImpl.createEvent(isCallConsideredEcho(WILDCARD_HANDLE),
          localReservations));
    }
    reservations = new ArrayList<PlacedReservation>(reservations);
    reservations.addAll(localReservations);
    return reservations;
  }

  @Override
  public void emptyCacheForQueue(String queue) throws LlamaException {
    am.emptyCacheForQueue(queue);
  }

  private synchronized List<PlacedReservation> gReleaseReservationsForQueue(
      String queue) {
    List<PlacedReservation> reservations = new ArrayList<PlacedReservation>();
    Iterator<PlacedReservationImpl> it =
        localReservations.values().iterator();
    while (it.hasNext()) {
      PlacedReservationImpl pr = it.next();
      if (pr.getQueue().equals(queue)) {
        it.remove();
        submittedReservations.remove(pr.getReservationId());
        pr.setStatus(PlacedReservation.Status.RELEASED);
        reservations.add(pr);
        LOG.debug(
            "Releasing all reservations for queue '{}', reservationId '{}'",
            queue, pr.getReservationId());
      }
    }
    return reservations;
  }

  @Override
  public void addListener(LlamaAMListener listener) {
    am.addListener(listener);
  }

  @Override
  public void removeListener(LlamaAMListener listener) {
    am.removeListener(listener);
  }

  @Override
  public synchronized void onEvent(LlamaAMEvent event) {
    LlamaAMEventImpl eventImpl = (LlamaAMEventImpl) event;
    for (PlacedResource resource : eventImpl.getResourceChanges()) {
      if (resource.getStatus() == PlacedResource.Status.ALLOCATED &&
          submittedReservations.contains(resource.getReservationId())) {
        timeOfLastAllocation = System.currentTimeMillis();
        LOG.debug("Resetting last resource allocation");
        break;
      }
    }
    for (PlacedReservation reservation : event.getReservationChanges()) {
      switch (reservation.getStatus()) {
        case ALLOCATED:
        case REJECTED:
        case PREEMPTED:
        case LOST:
          localReservations.remove(reservation.getReservationId());
          submittedReservations.remove(reservation.getReservationId());
          break;
      }
    }
  }

  @Override
  public void run() {
    try {
      Thread.sleep(noAllocationLimit);
    } catch (InterruptedException ex) {
      //NOP
    }
    while (isRunning()) {
      try {
        LOG.trace("Running deadlock avoidance");
        LlamaAMEventImpl  event = new LlamaAMEventImpl();
        long sleepTime;
        synchronized (this) {
          long sleepTime1 = deadlockAvoidance(event);
          long sleepTime2 = reReserveBackOffs(event);
          sleepTime = Math.min(sleepTime1, sleepTime2);
        }
        dispatch(event);

        LOG.trace("Deadlock avoidance thread sleeping for '{}' ms",
            sleepTime);
        Thread.sleep(sleepTime);
      } catch (InterruptedException ex) {
        //NOP
      }
    }
  }

  long deadlockAvoidance(LlamaAMEventImpl event) {
    long sleepTime;
    long timeWithoutAllocations = System.currentTimeMillis() -
        timeOfLastAllocation;
    if (timeWithoutAllocations >= noAllocationLimit) {
      doReservationsBackOff(event);
      sleepTime = noAllocationLimit;
    } else {
      LOG.debug("Recent allocation, '{}' ms ago, skipping back off",
          timeWithoutAllocations);
      sleepTime = timeWithoutAllocations;
    }
    return sleepTime;
  }

  long getBackOffDelay() {
    return backOffMinDelay + random.nextInt((int)
        (backOffMaxDelay - backOffMinDelay));
  }

  void doReservationsBackOff(LlamaAMEventImpl event) {
    if (submittedReservations.isEmpty()) {
      LOG.debug("No pending gang reservations to back off");
    } else {
      LOG.debug("Starting gang reservations back off");
      int numberOfGangResources = 0;
      List<UUID> submitted = new ArrayList<UUID>(submittedReservations);
      for (UUID id : submitted) {
        PlacedReservation pr = localReservations.get(id);
        if (pr != null) {
          numberOfGangResources += pr.getResources().size();
        }
      }
      int reservationsBackedOff = 0;
      int toGetRidOff = numberOfGangResources * backOffPercent / 100;
      int gotRidOff = 0;
      while (gotRidOff < toGetRidOff && !submitted.isEmpty()) {
        int victim = random.nextInt(submitted.size());
        UUID reservationId = submitted.get(victim);
        PlacedReservationImpl reservation = localReservations.get(reservationId);
        if (reservation != null) {
          try {
            LOG.warn(
                "Backing off gang reservation '{}' with '{}' resources",
                reservation.getReservationId(),
                reservation.getResources().size());
            am.releaseReservation(reservation.getHandle(),
                reservation.getReservationId(), true);
            reservation.setStatus(PlacedReservation.Status.BACKED_OFF);
            backedOffReservations.add(
                new BackedOffReservation(reservation, getBackOffDelay()));
            submittedReservations.remove(reservationId);
            submitted.remove(reservationId);

            event.addReservation(reservation);

            MetricUtil.meter(getMetricRegistry(), BACKED_OFF_RESERVATIONS_METER,
                1);
            MetricUtil.meter(getMetricRegistry(), BACKED_OFF_RESOURCES_METER,
                reservation.getResources().size());

          } catch (LlamaException ex) {
            LOG.warn("Error while backing off gang reservation '{}': {}",
                reservation.getReservationId(), ex.toString(), ex);
          }
          gotRidOff += reservation.getResources().size();
          reservationsBackedOff++;
        }
      }
      LOG.debug("Finishing gang reservations back off, backed off '{}' " +
          "reservations with '{}' resources", reservationsBackedOff, gotRidOff);
    }
    //resetting to current last allocation to start waiting again.
    timeOfLastAllocation = System.currentTimeMillis();
  }

  long reReserveBackOffs(LlamaAMEventImpl event) {
    BackedOffReservation br = backedOffReservations.poll();
    if (br != null) {
      LOG.debug("Starting re-reserving backed off gang reservations");
    } else {
      LOG.debug("No backed off gang reservation to re-reserve");
    }
    while (br != null) {
      UUID reservationId = br.getReservation().getReservationId();
      if (localReservations.containsKey(reservationId)) {
        try {
          LOG.info("Re-reserving gang reservation '{}'",
              br.getReservation().getReservationId());
          am.reserve(reservationId, br.getReservation());
          submittedReservations.add(reservationId);
        } catch (LlamaException ex) {
          localReservations.remove(reservationId);
          PlacedReservationImpl pr = br.getReservation();
          pr.setStatus(PlacedReservation.Status.REJECTED);
          event.addReservation(pr);
        }
      } else {
        LOG.warn(
            "Could not re-reserve '{}', not found in local reservations",
            reservationId);
      }
      br = backedOffReservations.poll();
    }
    br = backedOffReservations.peek();
    return (br != null) ? br.getDelay(TimeUnit.MILLISECONDS) : Long.MAX_VALUE;
  }
}

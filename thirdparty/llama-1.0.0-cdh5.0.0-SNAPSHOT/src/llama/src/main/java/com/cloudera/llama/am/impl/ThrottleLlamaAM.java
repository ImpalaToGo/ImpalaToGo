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
import com.cloudera.llama.am.api.LlamaAMListener;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.server.MetricUtil;
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.ErrorCode;
import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;import java.util.Map;

public class ThrottleLlamaAM extends LlamaAMImpl
    implements LlamaAMListener, IntraLlamaAMsCallback, Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ThrottleLlamaAM.class);

  private static final String LLAMA_PREFIX = LlamaAM.PREFIX_KEY + "throttling.";

  public static final String MAX_PLACED_RESERVATIONS_KEY = LLAMA_PREFIX +
      "maximum.placed.reservations";

  static final int MAX_PLACED_RESERVATIONS_DEFAULT = 20;

  public static final String MAX_QUEUED_RESERVATIONS_KEY = LLAMA_PREFIX +
      "maximum.queued.reservations";

  static final int MAX_QUEUED_RESERVATIONS_DEFAULT = 50;

  static final String MAX_PLACED_RESERVATIONS_QUEUE_KEY =
      MAX_PLACED_RESERVATIONS_KEY + ".{}";

  static final String MAX_QUEUED_RESERVATIONS_QUEUE_KEY =
      MAX_QUEUED_RESERVATIONS_KEY + ".{}";

  static final String METRIC_PREFIX_TEMPLATE = LlamaAM.METRIC_PREFIX +
      "queue({}).";

  private static final String PLACED_RESERVATIONS_GAUGE_TEMPLATE =
      METRIC_PREFIX_TEMPLATE + "throttle.placed-reservations.gauge";

  private static final String QUEUED_RESERVATIONS_GAUGE_TEMPLATE =
      METRIC_PREFIX_TEMPLATE + "throttle.queued-reservations.gauge";

  private final String queue;
  private final SingleQueueLlamaAM am;
  private IntraLlamaAMsCallback callback;
  private final int maxPlacedReservations;
  private final int maxQueuedReservations;
  private int placedReservations;
  private final Map<UUID, PlacedReservationImpl> queuedReservations;
  private Thread thread;
  private volatile boolean running;

  public ThrottleLlamaAM(Configuration conf, String queue,
      SingleQueueLlamaAM llamaAM) {
    super(conf);
    this.queue = queue;
    int defaultMaxPlacedRes = conf.getInt(MAX_PLACED_RESERVATIONS_KEY,
        MAX_PLACED_RESERVATIONS_DEFAULT);
    int defaultMaxQueuedRes = conf.getInt(MAX_QUEUED_RESERVATIONS_KEY,
        MAX_QUEUED_RESERVATIONS_DEFAULT);
    maxPlacedReservations = conf.getInt(FastFormat.format(
        MAX_PLACED_RESERVATIONS_QUEUE_KEY, queue), defaultMaxPlacedRes);
    maxQueuedReservations = conf.getInt(FastFormat.format(
        MAX_QUEUED_RESERVATIONS_QUEUE_KEY, queue), defaultMaxQueuedRes);
    LOG.info("Throttling queue '{}' max placed '{}' max queued '{}", queue,
        maxPlacedReservations, maxQueuedReservations);
    placedReservations = 0;
    queuedReservations = new LinkedHashMap<UUID, PlacedReservationImpl>();
    this.am = llamaAM;
    am.addListener(this);
    am.setCallback(this);
    thread = new Thread(this, "llama-am-throttle:" + queue);
    thread.setDaemon(true);
  }

  public void setCallback(IntraLlamaAMsCallback callback) {
    this.callback = callback;
  }

  @Override
  public void discardReservation(UUID reservationId) {
    callback.discardReservation(reservationId);
  }

  @Override
  public void discardAM(String queue) {
    callback.discardAM(queue);
  }

  int getMaxPlacedReservations() {
    return  maxPlacedReservations;
  }

  int getMaxQueuedReservations() {
    return maxQueuedReservations;
  }

  @Override
  public void setMetricRegistry(MetricRegistry metricRegistry) {
    super.setMetricRegistry(metricRegistry);
    am.setMetricRegistry(metricRegistry);
  }

  @Override
  public void start() throws LlamaException {
    am.start();
    if (getMetricRegistry() != null) {
      String key = FastFormat.format(PLACED_RESERVATIONS_GAUGE_TEMPLATE, queue);
      MetricUtil.registerGauge(getMetricRegistry(), key,
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              synchronized (this) {
                return placedReservations;
              }
            }
          });
      key = FastFormat.format(QUEUED_RESERVATIONS_GAUGE_TEMPLATE, queue);
      MetricUtil.registerGauge(getMetricRegistry(), key,
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              synchronized (this) {
                return queuedReservations.size();
              }
            }
          });
    }
    running = true;
    thread.start();
  }

  @Override
  public void stop() {
    running = false;
    thread.interrupt();
    if (getMetricRegistry() != null) {
      String key = FastFormat.format(PLACED_RESERVATIONS_GAUGE_TEMPLATE, queue);
      getMetricRegistry().remove(key);
      key = FastFormat.format(QUEUED_RESERVATIONS_GAUGE_TEMPLATE, queue);
      getMetricRegistry().remove(key);
    }
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

  synchronized int getPlacedReservations() {
    return placedReservations;
  }

  synchronized int getQueuedReservations() {
    return queuedReservations.size();
  }

  synchronized PlacedReservation throttle(UUID reservationId,
      Reservation reservation) throws LlamaException {
    PlacedReservationImpl pr = null;
    if (placedReservations >= maxPlacedReservations) {
      if (queuedReservations.size() >= maxQueuedReservations) {
        throw new LlamaException(ErrorCode.LLAMA_MAX_RESERVATIONS_FOR_QUEUE,
            queue, maxQueuedReservations);
      }
      pr = new PlacedReservationImpl(reservationId, reservation);
      pr.setQueued(true);
      queuedReservations.put(reservationId, pr);
      LOG.debug("Queuing '{}'", pr);
    } else {
      placedReservations++;
    }
    return pr;
  }

  synchronized PlacedReservation getThrottled(UUID reservationId) {
    return queuedReservations.get(reservationId);
  }

  synchronized PlacedReservation releaseThrottled(UUID handle,
      UUID reservationId) throws LlamaException {
    PlacedReservationImpl pr = queuedReservations.get(reservationId);
    if (pr != null) {
      if (handle.equals(pr.getHandle()) || isAdminCall()) {
        queuedReservations.remove(reservationId);
        pr.setStatus(PlacedReservation.Status.RELEASED);
        LOG.debug("Release queued '{}'", pr);
      } else {
        throw new LlamaException(ErrorCode.CLIENT_DOES_NOT_OWN_RESERVATION,
            handle, reservationId);
      }
    } else {
      pr = null;
    }
    return pr;
  }

  synchronized List<PlacedReservation> releaseThrottledForHandle(UUID handle) {
    List<PlacedReservation> list = new ArrayList<PlacedReservation>();
    Iterator<Map.Entry<UUID, PlacedReservationImpl>> it =
        queuedReservations.entrySet().iterator();
    int count = 0;
    while (it.hasNext()) {
      PlacedReservationImpl pr = it.next().getValue();
      if (pr.getHandle().equals(handle)) {
        it.remove();
        pr.setStatus(PlacedReservation.Status.RELEASED);
        list.add(pr);
        count++;
        LOG.debug("Release queued '{}'", pr);
      }
    }
    LOG.debug("Release '{}' reservations queued for handle '{}'", count, handle);
    return list;
  }

  synchronized List<PlacedReservation> releaseThrottledForQueue() {
    List<PlacedReservation> list = new ArrayList<PlacedReservation>();
    for (Map.Entry<UUID, PlacedReservationImpl> uuidPlacedReservationEntry :
        queuedReservations.entrySet()) {
      PlacedReservationImpl pr = uuidPlacedReservationEntry.getValue();
      pr.setStatus(PlacedReservation.Status.RELEASED);
      list.add(pr);
      LOG.debug("Release queued '{}'", pr);
    }
    queuedReservations.clear();
    LOG.debug("Release '{}' reservations queued for queue '{}'", list.size(),
        queue);
    return list;
  }

  synchronized void decreasePlaced(int count) {
    placedReservations -= count;
    if (placedReservations < maxPlacedReservations
        && !queuedReservations.isEmpty()) {
      thread.interrupt();
    }
  }

  @Override
  public void reserve(UUID reservationId, Reservation reservation)
      throws LlamaException {
    //TODO introduce QUEUED status
    PlacedReservation placedReservation = throttle(reservationId, reservation);
    if (placedReservation == null) {
      am.reserve(reservationId, reservation);
    } else {
      dispatch(LlamaAMEventImpl.createEvent(true, placedReservation));
    }
  }

  @Override
  public PlacedReservation getReservation(UUID reservationId)
      throws LlamaException {
    PlacedReservation placedReservation = getThrottled(reservationId);
    if (placedReservation == null) {
      placedReservation = am.getReservation(reservationId);
    }
    return placedReservation;
  }

  @Override
  public PlacedReservation releaseReservation(UUID handle, UUID reservationId,
      boolean doNotCache)
      throws LlamaException {
    PlacedReservation reservation = releaseThrottled(handle, reservationId);
    if (reservation == null) {
      reservation = am.releaseReservation(handle, reservationId, doNotCache);
    } else {
      dispatch(LlamaAMEventImpl.createEvent(isCallConsideredEcho(handle),
          reservation));
    }
    return reservation;
  }

  @Override
  public List<PlacedReservation> releaseReservationsForHandle(UUID handle,
      boolean doNotCache)
      throws LlamaException {
    List<PlacedReservation> localReservations =
        releaseThrottledForHandle(handle);
    List<PlacedReservation> reservations = new ArrayList<PlacedReservation>();
    reservations.addAll(localReservations);
    List<PlacedReservation> pReservations =
        am.releaseReservationsForHandle(handle, doNotCache);
    reservations.addAll(pReservations);
    if (!localReservations.isEmpty()) {
      dispatch(LlamaAMEventImpl.createEvent(isCallConsideredEcho(handle),
          localReservations));
    }
    return reservations;
  }

  @Override
  public List<PlacedReservation> releaseReservationsForQueue(
      String queue, boolean doNotCache) throws LlamaException {
    List<PlacedReservation> localReservations =
        releaseThrottledForQueue();
    List<PlacedReservation> reservations = new ArrayList<PlacedReservation>();
    reservations.addAll(localReservations);
    List<PlacedReservation> pReservations =
        am.releaseReservationsForQueue(queue, doNotCache);
    reservations.addAll(pReservations);
    if (!localReservations.isEmpty()) {
      dispatch(LlamaAMEventImpl.createEvent(isCallConsideredEcho(WILDCARD_HANDLE),
          localReservations));
    }
    return reservations;
  }

  @Override
  public void emptyCacheForQueue(String queue) throws LlamaException {
    am.emptyCacheForQueue(queue);
  }

  @Override
  public void onEvent(LlamaAMEvent event) {
    int count = 0;
    for (PlacedReservation r : event.getReservationChanges()) {
      if (r.getStatus().isFinal()) {
        count++;
      }
    }
    if (count > 0) {
      decreasePlaced(count);
    }
    dispatch(event);
  }

  @Override
  public void run() {
    while (running) {
      try {
        Clock.sleep(1000);
      } catch (InterruptedException ex) {
        LOG.trace("Interrupted");
      }
      placeThrottledReservations();
    }
  }

  synchronized void placeThrottledReservations() {
    LOG.trace("Running throttle for '{}'", queue);
    LlamaAMEventImpl events = new LlamaAMEventImpl();
    Iterator<PlacedReservationImpl> it = queuedReservations.values().iterator();
    int placed = 0;
    int failed = 0;
    while (placedReservations < maxPlacedReservations && it.hasNext()) {
      PlacedReservationImpl pr = it.next();
      it.remove();
      try {
        pr.setQueued(false);
        pr.setStatus(PlacedReservation.Status.PENDING);
        am.reserve(pr.getReservationId(), pr);
        events.addReservation(pr);
        placed++;
        placedReservations++;
      } catch (Throwable ex) {
        pr.setStatus(PlacedReservation.Status.REJECTED);
        events.addReservation(pr);
        failed++;
      }
    }
    if (placed + failed > 0) {
      LOG.debug("Placed '{}' reservations successfully and '{}' failed", placed,
          failed);
    }
    if (!events.getReservationChanges().isEmpty()) {
      dispatch(events);
    }
  }
}

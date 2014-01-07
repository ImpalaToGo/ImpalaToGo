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
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.server.MetricUtil;
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiQueueLlamaAM extends LlamaAMImpl implements LlamaAMListener,
    IntraLlamaAMsCallback {
  private static final Logger LOG = 
      LoggerFactory.getLogger(MultiQueueLlamaAM.class);

  private static final String QUEUES_GAUGE = METRIC_PREFIX + "queues.gauge";
  private static final String RESERVATIONS_GAUGE = METRIC_PREFIX +
      "reservations.gauge";
  private static final int AM_CHECK_EXPIRY_INTERVAL_MS = 5000;

  // Maps queue name to AM info. Visible for testing.
  final Map<String, SingleQueueAMInfo> ams;
  private SingleQueueLlamaAM llamaAMForGetNodes;
  private final Map<UUID, String> reservationToQueue;
  private volatile boolean running;
  private final int queueExpireMs;
  private final ExpireThread expireThread;
  // Visible for testing
  int amCheckExpiryIntervalMs;

  public MultiQueueLlamaAM(Configuration conf) {
    super(conf);
    ams = new ConcurrentHashMap<String, SingleQueueAMInfo>();
    reservationToQueue = new HashMap<UUID, String>();
    queueExpireMs = conf.getInt(QUEUE_AM_EXPIRE_MS,
        QUEUE_AM_EXPIRE_MS_DEFAULT);
    expireThread = new ExpireThread();
    amCheckExpiryIntervalMs = AM_CHECK_EXPIRY_INTERVAL_MS;
    if (SingleQueueLlamaAM.getRMConnectorClass(conf) == null) {
      throw new IllegalArgumentException(FastFormat.format(
          "RMConnector class not defined in the configuration under '{}'",
          SingleQueueLlamaAM.RM_CONNECTOR_CLASS_KEY));
    }
  }

  @Override
  public synchronized void setMetricRegistry(MetricRegistry metricRegistry) {
    super.setMetricRegistry(metricRegistry);
    for (SingleQueueAMInfo amInfo : ams.values()) {
      amInfo.am.setMetricRegistry(metricRegistry);
    }
    if (metricRegistry != null) {
      MetricUtil.registerGauge(metricRegistry, QUEUES_GAUGE,
          new CachedGauge<List<String>>(1, TimeUnit.SECONDS) {
            @Override
            protected List<String> loadValue() {
              synchronized (ams) {
                return new ArrayList<String>(ams.keySet());
              }
            }
          });
      MetricUtil.registerGauge(metricRegistry, RESERVATIONS_GAUGE,
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return reservationToQueue.size();
            }
          });
    }
  }

  // LlamaAMListener API

  @Override
  public void onEvent(LlamaAMEvent event) {
    dispatch(event);
  }

  // LlamaAM API

  private LlamaAM getLlamaAM(String queue, boolean create)
      throws LlamaException {
    return getSingleQueueAMInfo(queue, create, false).am;
  }
  
  private SingleQueueAMInfo getSingleQueueAMInfo(String queue, boolean create, boolean core)
      throws LlamaException {
    SingleQueueAMInfo amInfo;
    synchronized (ams) {
      amInfo = ams.get(queue);
      if (amInfo == null && create) {
        SingleQueueLlamaAM qAm = new SingleQueueLlamaAM(getConf(), queue);
        boolean throttling = getConf().getBoolean(
            THROTTLING_ENABLED_KEY,
            THROTTLING_ENABLED_DEFAULT);
        throttling = getConf().getBoolean(
            THROTTLING_ENABLED_KEY + "." + queue, throttling);
        LOG.info("Throttling for queue '{}' enabled '{}'", queue,
            throttling);
        LlamaAM am;
        if (throttling) {
          ThrottleLlamaAM tAm = new ThrottleLlamaAM(getConf(), queue, qAm);
          tAm.setCallback(this);
          am = tAm;
        } else {
          am = qAm;
        }
        am.setMetricRegistry(getMetricRegistry());
        am.start();
        am.addListener(this);
        amInfo = new SingleQueueAMInfo(am, core);
        ams.put(queue, amInfo);
      }
    }
    return amInfo;
  }

  private Set<SingleQueueAMInfo> getLlamaAMs() throws LlamaException {
    synchronized (ams) {
      return new HashSet<SingleQueueAMInfo>(ams.values());
    }
  }


  @Override
  public void start() throws LlamaException {
    for (String queue :
        getConf().getTrimmedStringCollection(CORE_QUEUES_KEY)) {
      try {
        getSingleQueueAMInfo(queue, true, true);
      } catch (LlamaException ex) {
        stop();
        throw ex;
      }
    }
    llamaAMForGetNodes = new SingleQueueLlamaAM(getConf(), null);
    llamaAMForGetNodes.start();

    running = true;
    expireThread.start();
  }

  private class ExpireThread extends Thread {
    public ExpireThread() {
      super("Queue AM Expiry Thread");
    }

    @Override
    public void run() {
      while (running) {
        long now = Clock.currentTimeMillis();
        Set<Map.Entry<String, SingleQueueAMInfo>> entries;
        synchronized (ams) {
          entries =
              new HashSet<Map.Entry<String, SingleQueueAMInfo>>(ams.entrySet());
        }
        for (Map.Entry<String, SingleQueueAMInfo> entry : entries) {
          SingleQueueAMInfo amInfo = entry.getValue();
          if (amInfo.isIdleTimeout(now)) {
            // Only need to synchronize if we want to remove the AM
            boolean removed = false;
            synchronized (ams) {
              if (amInfo.isIdleTimeout(now)) {
                LOG.info("Expiring AM for queue '{}'", entry.getKey());
                ams.remove(entry.getKey());
                removed = true;
              }
            }
            // Stopping requires communication with YARN so we don't
            // want to hold on to the lock while we're doing this.
            if (removed) {
              amInfo.am.stop();
            }
          }
        }

        try {
          Thread.sleep(amCheckExpiryIntervalMs);
        } catch (InterruptedException ex) {
          // Ignore
        }
      }
    }
  }

  @Override
  public void stop() {
    running = false;
    expireThread.interrupt();
    try {
      expireThread.join();
    } catch (InterruptedException ex) {
      LOG.warn("Interrupted while joining with ExpiryThread");
    }
    
    synchronized (ams) {
      for (SingleQueueAMInfo am : ams.values()) {
        am.am.stop();
      }
    }
    if (llamaAMForGetNodes != null) {
      llamaAMForGetNodes.stop();
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public List<String> getNodes() throws LlamaException {
    return llamaAMForGetNodes.getNodes();
  }

  @SuppressWarnings("deprecation")
  @Override
  public void reserve(UUID reservationId, Reservation reservation)
      throws LlamaException {
    SingleQueueAMInfo amInfo;
    // Get AM info and update num reservations atomically so that we don't destroy
    // the AM in between.
    synchronized (ams) {
      amInfo = getSingleQueueAMInfo(reservation.getQueue(), true, false);
      amInfo.incrementReservations();
    }
    amInfo.am.reserve(reservationId, reservation);
    reservationToQueue.put(reservationId, reservation.getQueue());
  }

  @SuppressWarnings("deprecation")
  @Override
  public PlacedReservation getReservation(UUID reservationId)
      throws LlamaException {
    PlacedReservation reservation = null;
    String queue = reservationToQueue.get(reservationId);
    if (queue != null) {
      LlamaAM am = getLlamaAM(queue, false);
      if (am != null) {
        reservation = am.getReservation(reservationId);
      } else {
        LOG.warn("Queue '{}' not available anymore", queue);
      }
    } else {
      LOG.warn("getReservation({}), reservationId not found",
          reservationId);
    }
    return reservation;
  }

  @SuppressWarnings("deprecation")
  @Override
  public PlacedReservation releaseReservation(UUID handle, UUID reservationId,
      boolean doNotCache) throws LlamaException {
    PlacedReservation pr = null;
    String queue = reservationToQueue.remove(reservationId);
    if (queue != null) {
      SingleQueueAMInfo amInfo = getSingleQueueAMInfo(queue, false, false);
      if (amInfo != null) {
        pr = amInfo.am.releaseReservation(handle, reservationId, doNotCache);
        if (pr != null) {
          amInfo.decrementReservations(1);
        }
      } else {
        LOG.warn("Queue '{}' not available anymore", queue);
      }
    } else {
      LOG.warn("releaseReservation({}), reservationId not found",
          reservationId);
    }
    return pr;
  }

  @Override
  public List<PlacedReservation> releaseReservationsForHandle(UUID handle,
      boolean doNotCache)
      throws LlamaException {
    List<PlacedReservation> reservations = new ArrayList<PlacedReservation>();
    LlamaException thrown = null;
    for (SingleQueueAMInfo amInfo : getLlamaAMs()) {
      try {
        List<PlacedReservation> released =
            amInfo.am.releaseReservationsForHandle(handle, doNotCache);
        if (!released.isEmpty()) {
          amInfo.decrementReservations(released.size());
        }
        reservations.addAll(released);
      } catch (LlamaException ex) {
        if (thrown != null) {
          LOG.error("releaseReservationsForHandle({}), error: {}",
              handle, ex.toString(), ex);
        }
        thrown = ex;
      }
    }
    if (thrown != null) {
      throw thrown;
    }
    return reservations;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<PlacedReservation> releaseReservationsForQueue(String queue,
      boolean doNotCache)
      throws LlamaException {
    List<PlacedReservation> list;
    LlamaAM am;
    synchronized (ams) {
      am = ((doNotCache) ? ams.remove(queue) : ams.get(queue)).am;
    }
    if (am != null) {
      list = am.releaseReservationsForQueue(queue, doNotCache);
      getSingleQueueAMInfo(queue, false, false).decrementReservations(
          list.size());
      if (doNotCache) {
        am.stop();
      }
    } else {
      list = Collections.EMPTY_LIST;
    }
    return list;
  }

  @Override
  public void emptyCacheForQueue(String queue) throws LlamaException {
    if (queue == ALL_QUEUES) {
      for (SingleQueueAMInfo amInfo : getLlamaAMs()) {
        amInfo.am.emptyCacheForQueue(queue);
      }
    } else {
      LlamaAM am = getLlamaAM(queue, false);
      if (am != null) {
        am.emptyCacheForQueue(queue);
      }
    }
  }

  @Override
  public void discardAM(String queue) {
    LOG.warn("discarding queue '{}' and all its reservations", queue);
    synchronized (ams) {
      ams.remove(queue);
      Iterator<Map.Entry<UUID, String>> i =
          reservationToQueue.entrySet().iterator();
      while (i.hasNext()) {
        if (i.next().getValue().equals(queue)) {
          i.remove();
        }
      }
    }
  }

  @Override
  public void discardReservation(UUID reservationId) {
    reservationToQueue.remove(reservationId);
  }

  private class SingleQueueAMInfo {
    public final LlamaAM am;
    private final AtomicInteger numReservations;
    // Whether we shouldn't delete this AM after it's empty for a while
    private final boolean core;
    // Time at which the AM became empty
    private volatile long emptyTime;

    public SingleQueueAMInfo(LlamaAM am, boolean core) {
      this.am = am;
      this.core = core;
      this.emptyTime = Long.MAX_VALUE;
      this.numReservations = new AtomicInteger(0);
    }

    public boolean isIdleTimeout(long now) {
      return !core && numReservations.get() == 0 &&
          now - emptyTime > queueExpireMs;
    }

    public void incrementReservations() {
      numReservations.incrementAndGet();
    }

    public void decrementReservations(int num) {
      int numRemaining = numReservations.addAndGet(-num);
      if (numRemaining == 0) {
        emptyTime = Clock.currentTimeMillis();
      }
    }
  }

}

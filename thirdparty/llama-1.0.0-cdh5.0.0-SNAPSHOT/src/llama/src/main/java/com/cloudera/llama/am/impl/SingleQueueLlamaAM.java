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
import com.cloudera.llama.am.cache.CacheRMConnector;
import com.cloudera.llama.util.ErrorCode;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.am.spi.RMEvent;
import com.cloudera.llama.am.spi.RMListener;
import com.cloudera.llama.am.spi.RMConnector;
import com.cloudera.llama.am.yarn.YarnRMConnector;
import com.cloudera.llama.server.MetricUtil;
import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.Gauge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleQueueLlamaAM extends LlamaAMImpl implements
    RMListener {
  private static final Logger LOG = 
      LoggerFactory.getLogger(SingleQueueLlamaAM.class);

  private static final String METRIC_PREFIX_TEMPLATE = LlamaAM.METRIC_PREFIX +
      "queue({}).";

  private static final String RESERVATIONS_GAUGE_TEMPLATE =
      METRIC_PREFIX_TEMPLATE + "reservations.gauge";
  private static final String RESOURCES_GAUGE_TEMPLATE =
      METRIC_PREFIX_TEMPLATE + "resources.gauge";
  private static final String RESERVATIONS_ALLOCATION_TIMER_TEMPLATE =
      METRIC_PREFIX_TEMPLATE + "reservations-allocation-time.timer";
  private static final String RESOURCES_ALLOCATION_TIMER_TEMPLATE =
      METRIC_PREFIX_TEMPLATE + "resources-allocation-time.timer";

  public static final List<String> METRIC_TEMPLATE_KEYS = Arrays.asList(
      RESERVATIONS_GAUGE_TEMPLATE, RESOURCES_GAUGE_TEMPLATE,
      RESERVATIONS_ALLOCATION_TIMER_TEMPLATE,
      RESOURCES_ALLOCATION_TIMER_TEMPLATE);

  private final String queue;
  private final Map<UUID, PlacedReservationImpl> reservationsMap;
  private final Map<UUID, PlacedResourceImpl> resourcesMap;
  private IntraLlamaAMsCallback callback;
  private String reservationsAllocationTimerKey;
  private String resourcesAllocationTimerKey;
  private RMConnector rmConnector;
  private boolean running;

  public static Class<? extends RMConnector> getRMConnectorClass(
      Configuration conf) {
    return conf.getClass(RM_CONNECTOR_CLASS_KEY, YarnRMConnector.class,
        RMConnector.class);
  }

  public SingleQueueLlamaAM(Configuration conf, String queue) {
    super(conf);
    this.queue = queue;
    reservationsMap = new HashMap<UUID, PlacedReservationImpl>();
    resourcesMap = new HashMap<UUID, PlacedResourceImpl>();
  }

  public void setCallback(IntraLlamaAMsCallback callback) {
    this.callback = callback;
  }

  // LlamaAM API

  @Override
  public void start() throws LlamaException {
    Class<? extends RMConnector> klass = getRMConnectorClass(getConf());
    rmConnector = ReflectionUtils.newInstance(klass, getConf());

    // queue is null only for the AM used to report getNodes(),
    // we don't need caching for it TODO and no normalization either when done
    if (queue != null) {
      boolean caching = getConf().getBoolean(
          CACHING_ENABLED_KEY,
          CACHING_ENABLED_DEFAULT);
      boolean normalizing = getConf().getBoolean(
          NORMALIZING_ENABLED_KEY,
          NORMALIZING_ENABLED_DEFAULT);
      caching = getConf().getBoolean(
          CACHING_ENABLED_KEY + "." + queue, caching);
      LOG.info("Caching for queue '{}' enabled '{}'", queue,
          caching);
      if (caching && normalizing) {
        CacheRMConnector connectorCache =
            new CacheRMConnector(getConf(), rmConnector);
        rmConnector = connectorCache;
      } else if (caching) {
        LOG.warn("Caching not allowed without normalization. To enable caching," +
            "set '{}' to true.", LlamaAM.NORMALIZING_ENABLED_KEY);
      }
      if (normalizing) {
        NormalizerRMConnector normalizer =
            new NormalizerRMConnector(getConf(), rmConnector);
        rmConnector = normalizer;
      }
    }
    rmConnector.setMetricRegistry(getMetricRegistry());
    rmConnector.setLlamaAMCallback(this);
    rmConnector.start();
    if (queue != null) {
      rmConnector.register(queue);
    }
    running = true;

    if (getMetricRegistry() != null) {
      String key = FastFormat.format(RESERVATIONS_GAUGE_TEMPLATE, queue);
      MetricUtil.registerGauge(getMetricRegistry(), key, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              synchronized (this) {
                return reservationsMap.size();
              }
            }
          });

      key = FastFormat.format(RESOURCES_GAUGE_TEMPLATE, queue);
      MetricUtil.registerGauge(getMetricRegistry(), key, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              synchronized (this) {
                return resourcesMap.size();
              }
            }
          });


      key = FastFormat.format(RESERVATIONS_ALLOCATION_TIMER_TEMPLATE, queue);
      MetricUtil.registerTimer(getMetricRegistry(), key);
      reservationsAllocationTimerKey = key;

      key = FastFormat.format(RESOURCES_ALLOCATION_TIMER_TEMPLATE, queue);
      MetricUtil.registerTimer(getMetricRegistry(), key);
      resourcesAllocationTimerKey = key;
    }
  }

  public RMConnector getRMConnector() {
    return rmConnector;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public synchronized void stop() {
    running = false;
    if (getMetricRegistry() != null) {
      if (getMetricRegistry() != null) {
        String key = FastFormat.format(RESERVATIONS_GAUGE_TEMPLATE, queue);
        getMetricRegistry().remove(key);
        key = FastFormat.format(RESOURCES_GAUGE_TEMPLATE, queue);
        getMetricRegistry().remove(key);
        key = FastFormat.format(RESERVATIONS_ALLOCATION_TIMER_TEMPLATE, queue);
        getMetricRegistry().remove(key);
        key = FastFormat.format(RESOURCES_ALLOCATION_TIMER_TEMPLATE, queue);
        getMetricRegistry().remove(key);
      }
    }
    if (rmConnector != null) {
      if (queue != null) {
        rmConnector.unregister();
      }
      rmConnector.stop();
    }
  }

  @Override
  public List<String> getNodes() throws LlamaException {
    return rmConnector.getNodes();
  }

  private void _addReservation(PlacedReservationImpl reservation) {
    UUID reservationId = reservation.getReservationId();
    reservationsMap.put(reservationId, reservation);
    for (PlacedResourceImpl resource : reservation.getPlacedResourceImpls()) {
      resource.setStatus(PlacedResource.Status.PENDING);
      resourcesMap.put(resource.getResourceId(), resource);
    }
  }

  PlacedReservationImpl _getReservation(UUID reservationId) {
    return reservationsMap.get(reservationId);
  }

  private PlacedReservationImpl _deleteReservation(UUID reservationId,
      PlacedReservation.Status status) {
    PlacedReservationImpl reservation = reservationsMap.remove(reservationId);
    if (reservation != null) {
      for (PlacedResource resource : reservation.getPlacedResources()) {
        resourcesMap.remove(resource.getResourceId());
      }
    }
    callback.discardReservation(reservationId);
    if (reservation != null) {
      reservation.setStatus(status);
    }
    return reservation;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void reserve(UUID reservationId,
      final Reservation reservation)
      throws LlamaException {
    final PlacedReservationImpl impl = new PlacedReservationImpl(reservationId,
        reservation);
    LlamaAMEvent event = LlamaAMEventImpl.createEvent(true, impl);
    synchronized (this) {
      _addReservation(impl);
    }
    try {
      rmConnector.reserve((List)impl.getPlacedResourceImpls());
    } catch (LlamaException ex) {
      synchronized (this) {
        _deleteReservation(impl.getReservationId(),
            PlacedReservation.Status.REJECTED);
      }
      throw ex;
    }
    dispatch(event);
  }

  @Override
  public PlacedReservation getReservation(final UUID reservationId)
      throws LlamaException {
    synchronized (this) {
      return _getReservation(reservationId);
    }
  }

  @Override
  public PlacedReservation releaseReservation(UUID handle,
      final UUID reservationId, boolean doNotCache)
      throws LlamaException {
    return releaseReservation(handle, reservationId, doNotCache, false);
  }

  @SuppressWarnings("unchecked")
  public PlacedReservation releaseReservation(UUID handle,
      final UUID reservationId, boolean doNotCache, boolean doNotDispatch)
      throws LlamaException {
    PlacedReservationImpl reservation;
    LlamaAMEvent event = null;
    synchronized (this) {
      reservation = _getReservation(reservationId);
      if (reservation != null) {
        if (!reservation.getHandle().equals(handle) && !isAdminCall()) {
          throw new LlamaException(ErrorCode.CLIENT_DOES_NOT_OWN_RESERVATION,
              handle, reservation.getReservationId());
        }
        reservation = _deleteReservation(reservationId,
            PlacedReservation.Status.RELEASED);
        event = LlamaAMEventImpl.createEvent(isCallConsideredEcho(handle),
            reservation);
      }
    }
    if (reservation != null) {
      rmConnector.release((List<RMResource>) (List) reservation.getResources(),
          doNotCache);
      if (!doNotDispatch) {
        dispatch(event);
      }
    } else {
      LOG.warn("Unknown reservationId '{}'", reservationId);
    }
    return reservation;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<PlacedReservation> releaseReservationsForHandle(UUID handle,
      boolean doNotCache)
      throws LlamaException {
    List<PlacedReservation> reservations = new ArrayList<PlacedReservation>();
    synchronized (this) {
      for (PlacedReservation reservation :
          new ArrayList<PlacedReservation>(reservationsMap.values())) {
        if (reservation.getHandle().equals(handle)) {
          reservation = _deleteReservation(reservation.getReservationId(),
              PlacedReservation.Status.RELEASED);
          reservations.add(reservation);
          LOG.debug(
              "Releasing all reservations for handle '{}', reservationId '{}'",
              handle, reservation.getReservationId());
        }
      }
    }
    for (PlacedReservation reservation : reservations) {
      rmConnector.release((List<RMResource>) (List) reservation.getResources(),
          doNotCache);
    }
    if (!reservations.isEmpty()) {
      dispatch(LlamaAMEventImpl.createEvent(isCallConsideredEcho(handle),
          reservations));
    }
    return reservations;
  }

  @Override
  public List<PlacedReservation> releaseReservationsForQueue(String queue,
      boolean doNotCache)
      throws LlamaException {
    List<PlacedReservation> reservations;
    synchronized (this) {
      reservations = new ArrayList<PlacedReservation>(reservationsMap.values());
      for (PlacedReservation res : reservations) {
        releaseReservation(res.getHandle(), res.getReservationId(), doNotCache,
            true);
        LOG.debug(
            "Releasing all reservations for queue '{}', reservationId '{}'",
            queue, res.getReservationId());
      }
    }
    if (!reservations.isEmpty()) {
      dispatch(LlamaAMEventImpl.createEvent(isCallConsideredEcho(WILDCARD_HANDLE),
          reservations));
    }
    return reservations;
  }

  @Override
  public void emptyCacheForQueue(String queue) throws LlamaException {
    rmConnector.emptyCache();
  }

  // PRIVATE METHODS

  private List<PlacedResourceImpl> _resourceRejected(
      PlacedResourceImpl resource, LlamaAMEventImpl event) {
    List<PlacedResourceImpl> toRelease = null;
    resource.setStatus(PlacedResource.Status.REJECTED);
    UUID reservationId = resource.getReservationId();
    PlacedReservationImpl reservation = reservationsMap.get(reservationId);
    if (reservation == null) {
      LOG.warn("Unknown Reservation '{}' during resource '{}' rejection " +
          "handling", reservationId, resource.getResourceId());
    } else {
      // if reservation is ALLOCATED, or it is PARTIAL and not GANG we let it be
      // and in the ELSE we notify the resource rejection
      switch (reservation.getStatus()) {
        case PENDING:
        case PARTIAL:
          if (reservation.isGang()) {
            reservation = _deleteReservation(reservationId,
                PlacedReservation.Status.REJECTED);
            toRelease = reservation.getPlacedResourceImpls();
            event.addReservation(reservation);
          }
          event.addResource(resource);
          break;
        case ALLOCATED:
          LOG.warn("Illegal internal state, reservation '{}' is " +
              "ALLOCATED, resource cannot  be rejected '{}'", reservationId,
              resource.getResourceId());
          break;
      }
    }
    return toRelease;
  }

  private void _resourceAllocated(PlacedResourceImpl resource,
      RMEvent change, LlamaAMEventImpl event) {
    resource.setAllocationInfo(change.getLocation(), change.getCpuVCores(),
        change.getMemoryMbs());
    UUID reservationId = resource.getReservationId();
    PlacedReservationImpl reservation = reservationsMap.get(reservationId);
    if (reservation == null) {
      LOG.warn("Reservation '{}' during resource allocation handling " +
          "for" + " '{}'", reservationId, resource.getResourceId());
    } else {

      MetricUtil.time(getMetricRegistry(), resourcesAllocationTimerKey,
          System.currentTimeMillis() - reservation.getPlacedOn(),
          new ReservationResourceLogContext(resource));

      List<PlacedResourceImpl> resources = reservation.getPlacedResourceImpls();
      boolean fulfilled = true;
      for (int i = 0; fulfilled && i < resources.size(); i++) {
        fulfilled = resources.get(i).getStatus() == PlacedResource.Status
            .ALLOCATED;
      }
      if (fulfilled) {
        reservation.setStatus(PlacedReservation.Status.ALLOCATED);

        MetricUtil.time(getMetricRegistry(), reservationsAllocationTimerKey,
            System.currentTimeMillis() - reservation.getPlacedOn(),
            new ReservationResourceLogContext(reservation));
      } else {
        reservation.setStatus(PlacedReservation.Status.PARTIAL);
      }
      event.addReservation(reservation);
      event.addResource(resource);
    }
  }

  private List<PlacedResourceImpl> _resourcePreempted(
      PlacedResourceImpl resource, LlamaAMEventImpl event) {
    List<PlacedResourceImpl> toRelease = null;
    resource.setStatus(PlacedResource.Status.PREEMPTED);
    UUID reservationId = resource.getReservationId();
    PlacedReservationImpl reservation = reservationsMap.get(reservationId);
    if (reservation == null) {
      LOG.warn("Unknown Reservation '{}' during resource preemption " +
          "handling for" + " '{}'", reservationId, resource.getResourceId());
    } else {
      switch (reservation.getStatus()) {
        case ALLOCATED:
          event.addResource(resource);
          break;
        case PARTIAL:
          if (reservation.isGang()) {
            _deleteReservation(reservationId,
                PlacedReservation.Status.PREEMPTED);
            toRelease = reservation.getPlacedResourceImpls();
            event.addReservation(reservation);
          } else {
            event.addResource(resource);
          }
          break;
        case PENDING:
          LOG.warn("Illegal internal state, reservation '{}' is PENDING, " +
              "resource '{}' cannot  be preempted, releasing reservation ",
              reservationId, resource.getResourceId());
          reservation = _deleteReservation(reservationId,
              PlacedReservation.Status.PREEMPTED);
          toRelease = reservation.getPlacedResourceImpls();
          event.addReservation(reservation);
          break;
      }
    }
    return toRelease;
  }

  private List<PlacedResourceImpl> _resourceLost(
      PlacedResourceImpl resource, LlamaAMEventImpl event) {
    List<PlacedResourceImpl> toRelease = null;
    resource.setStatus(PlacedResource.Status.LOST);
    UUID reservationId = resource.getReservationId();
    PlacedReservationImpl reservation = reservationsMap.get(reservationId);
    if (reservation == null) {
      LOG.warn("Unknown Reservation '{}' during resource lost handling " +
          "for '{}'", reservationId, resource.getResourceId());
    } else {
      switch (reservation.getStatus()) {
        case ALLOCATED:
          event.addResource(resource);
          break;
        case PARTIAL:
          if (reservation.isGang()) {
            reservation = _deleteReservation(reservationId,
                PlacedReservation.Status.LOST);
            toRelease = reservation.getPlacedResourceImpls();
            event.addReservation(reservation);
          } else {
            event.addResource(resource);
          }
          break;
        case PENDING:
          LOG.warn("RM lost reservation '{}' with resource '{}', " +
              "rejecting reservation", reservationId,
              resource.getResourceId());
          reservation = _deleteReservation(reservationId,
              PlacedReservation.Status.LOST);
          toRelease = reservation.getPlacedResourceImpls();
          event.addReservation(reservation);
          break;
      }
    }
    return toRelease;
  }

  // RMListener API

  @Override
  @SuppressWarnings("unchecked")
  public void onEvent(final List<RMEvent> rmEvents) {
    if (rmEvents == null) {
      throw new IllegalArgumentException("changes cannot be NULL");
    }
    LOG.trace("onEvent({})", rmEvents);
    LlamaAMEventImpl llamaAMEvent = new LlamaAMEventImpl();
    List<PlacedResourceImpl> toRelease = new ArrayList<PlacedResourceImpl>();
    synchronized (this) {
      for (RMEvent change : rmEvents) {
        PlacedResourceImpl resource = resourcesMap.get(change
            .getResourceId());
        if (resource == null) {
          LOG.warn("Unknown resource '{}'", change.getResourceId());
        } else {
          List<PlacedResourceImpl> release = null;
          switch (change.getStatus()) {
            case REJECTED:
              release = _resourceRejected(resource, llamaAMEvent);
              break;
            case ALLOCATED:
              _resourceAllocated(resource, change, llamaAMEvent);
              break;
            case PREEMPTED:
              release = _resourcePreempted(resource, llamaAMEvent);
              break;
            case LOST:
              release = _resourceLost(resource, llamaAMEvent);
              break;
          }
          if (release != null) {
            toRelease.addAll(release);
          }
        }
      }
    }
    if (!toRelease.isEmpty()) {
      try {
        rmConnector.release((List<RMResource>) (List) toRelease, false);
      } catch (LlamaException ex) {
        LOG.warn("release() error: {}", ex.toString(), ex);
      }
    }
    dispatch(llamaAMEvent);
  }

  //visible for testing only
  void loseAllReservations() {
    synchronized (this) {
      List<UUID> clientResourceIds = new ArrayList<UUID>(resourcesMap.keySet());
      List<RMEvent> changes = new ArrayList<RMEvent>();
      for (UUID clientResourceId : clientResourceIds) {
        changes.add(RMEvent.createStatusChangeEvent(clientResourceId,
            PlacedResource.Status.LOST));
      }
      onEvent(changes);
    }
  }

  @Override
  public void stoppedByRM() {
    LOG.warn("Stopped by '{}'", rmConnector.getClass().getSimpleName());
    loseAllReservations();
    callback.discardAM(queue);
  }

}

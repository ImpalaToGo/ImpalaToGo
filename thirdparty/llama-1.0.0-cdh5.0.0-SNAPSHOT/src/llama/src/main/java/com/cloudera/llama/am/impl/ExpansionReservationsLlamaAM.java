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

import com.cloudera.llama.am.api.Expansion;
import com.cloudera.llama.am.api.LlamaAM;
import com.cloudera.llama.am.api.LlamaAMEvent;
import com.cloudera.llama.am.api.LlamaAMListener;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.util.ErrorCode;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExpansionReservationsLlamaAM extends LlamaAMImpl implements LlamaAMListener {
  private static final Logger LOG = LoggerFactory.getLogger(
      ExpansionReservationsLlamaAM.class);

  private LlamaAM am;
  private Map<UUID, List<UUID>> reservationToExpansionsMap;

  public ExpansionReservationsLlamaAM(LlamaAM am) {
    super(am.getConf());
    this.am = am;
    am.addListener(this);
    reservationToExpansionsMap = new HashMap<UUID, List<UUID>>();
  }

  @Override
  public void setMetricRegistry(MetricRegistry metricRegistry) {
    super.setMetricRegistry(metricRegistry);
    am.setMetricRegistry(metricRegistry);
  }

  @Override
  public void start() throws LlamaException {
    am.start();
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
    am.reserve(reservationId, reservation);
  }

  synchronized void add(UUID reservationId, UUID expansionId) {
    List<UUID> expansions = reservationToExpansionsMap.get(reservationId);
    if (expansions == null) {
      expansions = new ArrayList<UUID>();
      reservationToExpansionsMap.put(reservationId, expansions);
    }
    expansions.add(expansionId);
  }

  synchronized List<UUID> removeExpansionsOf(UUID reservationId) {
    return reservationToExpansionsMap.remove(reservationId);
  }

  synchronized void removeExpansion(UUID reservationId, UUID expansionId) {
    List<UUID> eIds = reservationToExpansionsMap.get(reservationId);
    if (eIds != null) {
      eIds.remove(expansionId);
      if (eIds.isEmpty()) {
        reservationToExpansionsMap.remove(reservationId);
      }
    }
  }

  synchronized List<UUID> getExpansions(UUID reservationId) {
    return reservationToExpansionsMap.get(reservationId);
  }

  @Override
  public void expand(UUID expansionId, Expansion expansion)
      throws LlamaException {
    UUID reservationId = expansion.getExpansionOf();
    PlacedReservation originalReservation = am.getReservation(reservationId);
    if (originalReservation == null) {
      throw new LlamaException(ErrorCode.UNKNOWN_RESERVATION_FOR_EXPANSION,
          reservationId);
    }
    if (originalReservation.getExpansionOf() != null) {
      throw new LlamaException(ErrorCode.CANNOT_EXPAND_AN_EXPANSION_RESERVATION,
          reservationId);
    }
    if (originalReservation.getStatus() != PlacedReservation.Status.ALLOCATED) {
      throw new LlamaException(ErrorCode.CANNOT_EXPAND_A_RESERVATION_NOT_ALLOCATED,
          reservationId, originalReservation.getStatus());
    }
    Reservation reservation =
        PlacedReservationImpl.createReservationForExpansion(
            originalReservation, expansion);
    am.reserve(expansionId, reservation);
    add(reservationId, expansionId);
    if (am.getReservation(reservationId) == null) {
      am.releaseReservation(expansion.getHandle(), expansionId, false);
      throw new LlamaException(ErrorCode.UNKNOWN_RESERVATION_FOR_EXPANSION,
          reservationId);
    }
  }

  @Override
  public PlacedReservation getReservation(UUID reservationId)
      throws LlamaException {
    return am.getReservation(reservationId);
  }

  private void releaseExpansions(UUID reservationId, boolean doNotCache) {
    List<UUID> expansionIds = removeExpansionsOf(reservationId);
    if (expansionIds != null) {
      LOG.debug("Releasing reservation '{}' expansions '{}'", reservationId,
          expansionIds);
      for (UUID expansionId : expansionIds) {
        try {
          //events generated by this release should never be echo
          am.releaseReservation(WILDCARD_HANDLE, expansionId, doNotCache);
        } catch (Exception ex) {
          LOG.error("Could not release properly expansion '{}' for " +
              "reservation '{}': {}", expansionId, reservationId, ex.toString(),
              ex);
        }
      }
    }
  }

  private void releaseReservationAndExpansions(PlacedReservation reservation,
      boolean doNotCache) {
    if (reservation.getExpansionOf() == null) {
      releaseExpansions(reservation.getReservationId(), doNotCache);
    } else {
      removeExpansion(reservation.getExpansionOf(), reservation.getReservationId());
    }
  }

  @Override
  public PlacedReservation releaseReservation(UUID handle, UUID reservationId,
      boolean doNotCache) throws LlamaException {
    PlacedReservation reservation = am.getReservation(reservationId);
    if (reservation != null) {
      releaseReservationAndExpansions(reservation, doNotCache);
    }
    return am.releaseReservation(handle, reservationId, doNotCache);
  }

  @Override
  public List<PlacedReservation> releaseReservationsForHandle(UUID handle,
      boolean doNotCache) throws LlamaException {
    List<PlacedReservation> reservations =
        am.releaseReservationsForHandle(handle, doNotCache);
    for (PlacedReservation reservation : reservations) {
      releaseReservationAndExpansions(reservation, doNotCache);
    }
    return reservations;
  }

  @Override
  public List<PlacedReservation> releaseReservationsForQueue(String queue,
      boolean doNotCache) throws LlamaException {
    List<PlacedReservation> reservations =
        am.releaseReservationsForQueue(queue, doNotCache);
    for (PlacedReservation reservation : reservations) {
      //all expansions are in the same queue, the underlying AM released them
      //already, we just need to clean up the crossref
      removeExpansionsOf(reservation.getReservationId());
    }
    return reservations;
  }

  @Override
  public void emptyCacheForQueue(String queue) throws LlamaException {
    am.emptyCacheForQueue(queue);
  }

  @Override
  public void onEvent(LlamaAMEvent event) {
    if (!event.isEcho()) {
      for (PlacedReservation reservation : event.getReservationChanges()) {
        if (reservation.getStatus().isFinal()) {
          releaseExpansions(reservation.getReservationId(), false);
        }
      }
    }
    dispatch(event);
  }

}

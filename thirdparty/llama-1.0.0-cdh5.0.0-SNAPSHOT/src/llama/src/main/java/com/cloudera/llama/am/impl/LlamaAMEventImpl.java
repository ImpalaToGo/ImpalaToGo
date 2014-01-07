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

import com.cloudera.llama.am.api.LlamaAMEvent;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LlamaAMEventImpl implements LlamaAMEvent {
  private final List<PlacedResource> resources;
  private final List<PlacedReservation> reservations;
  private boolean echo;

  public LlamaAMEventImpl() {
    this(false);
  }

  public LlamaAMEventImpl(boolean echo) {
    resources = new ArrayList<PlacedResource>();
    reservations = new ArrayList<PlacedReservation>();
    this.echo = echo;
  }

  public void addReservation(PlacedReservation reservation) {
    reservations.add(new PlacedReservationImpl(reservation));
  }

  public void addReservations(List<PlacedReservation> reservations) {
    for (PlacedReservation reservation : reservations) {
      addReservation(reservation);
    }
  }

  public void addResource(PlacedResource resource) {
    resources.add(new PlacedResourceImpl(resource));
  }

  @Override
  public boolean isEmpty() {
    return reservations.isEmpty() && resources.isEmpty();
  }

  @Override
  public boolean isEcho() {
    return echo;
  }

  @Override
  public List<PlacedReservation> getReservationChanges() {
    return Collections.unmodifiableList(reservations);
  }

  @Override
  public List<PlacedResource> getResourceChanges() {
    return Collections.unmodifiableList(resources);
  }

  public static LlamaAMEvent merge(List<LlamaAMEvent> events) {
    LlamaAMEventImpl merged = new LlamaAMEventImpl();
    for (LlamaAMEvent event : events) {
      merged.reservations.addAll(event.getReservationChanges());
      merged.resources.addAll(event.getResourceChanges());
    }
    return merged;
  }

  public static LlamaAMEvent createEvent(boolean echo, PlacedReservation pr) {
    LlamaAMEventImpl e = new LlamaAMEventImpl(echo);
    e.addReservation(pr);
    return e;
  }

  public static LlamaAMEvent createEvent(boolean echo,
      List<PlacedReservation> prs) {
    LlamaAMEventImpl e = new LlamaAMEventImpl(echo);
    for (PlacedReservation pr : prs) {
      e.addReservation(pr);
    }
    return e;
  }

}

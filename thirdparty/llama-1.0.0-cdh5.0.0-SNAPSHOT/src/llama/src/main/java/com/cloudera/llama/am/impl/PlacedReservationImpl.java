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
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.am.api.Resource;
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.util.ParamChecker;
import com.cloudera.llama.util.UUID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PlacedReservationImpl implements PlacedReservation, Expansion {
  protected UUID reservationId;
  protected Status status;
  protected long placedOn;
  protected UUID handle;
  protected String user;
  protected String queue;
  protected boolean gang;
  protected List<PlacedResourceImpl> resources;
  protected UUID expansionOf;
  protected long allocatedOn;
  protected boolean queued;

  private PlacedReservationImpl() {
  }

  @SuppressWarnings("unchecked")
  private PlacedReservationImpl(
      UUID reservationId,
      Status status,
      long placedOn,
      UUID handle,
      String user,
      String queue,
      boolean gang,
      List<PlacedResourceImpl> resources,
      UUID expansionOf,
      long allocatedOn,
      boolean queued
  ) {
    this.reservationId = reservationId;
    this.status = status;
    this.placedOn = placedOn;
    this.handle = handle;
    this.user = user;
    this.queue = queue;
    this.gang = gang;
    this.resources = (resources != null) ? resources : Collections.EMPTY_LIST;
    this.expansionOf = expansionOf;
    this.allocatedOn = allocatedOn;
    this.queued = queued;

  }

  @SuppressWarnings("unchecked")
  public PlacedReservationImpl(PlacedReservation r) {
    this(r.getReservationId(),
        r.getStatus(),
        r.getPlacedOn(),
        r.getHandle(),
        r.getUser(),
        r.getQueue(),
        r.isGang(),
        copyResources(r.getPlacedResources()),
        r.getExpansionOf(),
        r.getAllocatedOn(), r.isQueued());
  }

  @SuppressWarnings("unchecked")
  public PlacedReservationImpl(UUID reservationId, Reservation reservation) {
    this(reservationId,
        Status.PENDING,
        (((PlacedReservation)reservation).getPlacedOn() != 0)
        ? ((PlacedReservation) reservation).getPlacedOn()
        : Clock.currentTimeMillis(),
        reservation.getHandle(),
        reservation.getUser(),
        reservation.getQueue(),
        reservation.isGang(),
        new ArrayList<PlacedResourceImpl>(),
        null,
        -1, false);
    for (Resource resource : reservation.getResources()) {
      resources.add(PlacedResourceImpl.createPlaced(this, resource));
    }
  }

  private static List<PlacedResourceImpl> copyResources(
      List<? extends PlacedResource> resources) {
    List<PlacedResourceImpl> list = null;
    if (resources != null) {
      list = new ArrayList<PlacedResourceImpl>();
      for (PlacedResource resource : resources) {
        list.add(new PlacedResourceImpl(resource));
      }
    }
    return list;
  }

  private static final String RESERVATION_TO_STRING =
      "Reservation[handle:{} user:{} queue:{} gang:{} resources:{}]";

  private static final String EXPANSION_TO_STRING =
      "Expansion[expansionOf:{} resource:{}]";

  private static final String PLACED_RESERVATION_TO_STRING =
      "PlacedReservation[reservationId:{} status:{} placedOn:{} " +
          "allocatedOn:{} expansionOf:{} handle:{} user:{} queue:{} gang:{} " +
          "queued:{} resources:{}]";

  @Override
  public String toString() {
    String str;
    if (getPlacedOn() == -1) {
        str = FastFormat.format(RESERVATION_TO_STRING, getHandle(), getUser(),
            getQueue(), isGang(), getResources());
    } else if (getExpansionOf() != null) {
        str = FastFormat.format(EXPANSION_TO_STRING, getExpansionOf(),
            getResource());
    } else {
        str = FastFormat.format(PLACED_RESERVATION_TO_STRING,
            getReservationId(), getStatus(), getPlacedOn(), getAllocatedOn(),
            getExpansionOf(), getHandle(), getUser(), getQueue(), isGang(),
            isQueued(), getResources());
    }
    return str;
  }

  @Override
  public boolean equals(Object obj) {
    boolean eq = false;
    if (obj instanceof PlacedReservationImpl) {
      eq = (this == obj) || (getReservationId() != null &&
          getReservationId().equals(
              ((PlacedReservationImpl) obj).getReservationId()));
    }
    return eq;
  }

  @Override
  public int hashCode() {
    return (getReservationId() != null) ? getReservationId().hashCode()
                                        : super.hashCode();
  }

  @Override
  public UUID getReservationId() {
    return reservationId;
  }

  @Override
  public UUID getExpansionOf() {
    return expansionOf;
  }

  @Override
  public Status getStatus() {
    return status;
  }

  @Override
  public long getPlacedOn() {
    return placedOn;
  }

  @Override
  public UUID getHandle() {
    return handle;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public String getQueue() {
    return queue;
  }

  @Override
  public boolean isGang() {
    return gang;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Resource> getResources() {
    return (List) resources;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<PlacedResource> getPlacedResources() {
    return (List) getResources();
  }

  @Override
  public Resource getResource() {
    return getResources().get(0);
  }

  @Override
  public long getAllocatedOn() {
    return allocatedOn;
  }

  @SuppressWarnings("unchecked")
  public List<PlacedResourceImpl> getPlacedResourceImpls() {
    return (List) getResources();
  }

  public void setStatus(Status status) {
    this.status = status;
    if (status == Status.ALLOCATED) {
      allocatedOn = Clock.currentTimeMillis();
    }
  }

  @Override
  public boolean isQueued() {
    return queued;
  }

  public void setQueued(boolean queued) {
    this.queued = queued;
  }

  public static class XReservationBuilder extends PlacedReservationImpl
      implements Reservation.Builder {
    private List<Resource> resources;

    public XReservationBuilder() {
      resources = new ArrayList<Resource>();
    }

    @Override
    public Reservation.Builder setHandle(UUID handle) {
      ParamChecker.notNull(handle, "handle");
      this.handle = handle;
      return this;
    }

    @Override
    public Reservation.Builder setUser(String user) {
      ParamChecker.notEmpty(user, "user");
      this.user = user;
      return this;
    }

    @Override
    public Reservation.Builder setQueue(String queue) {
      ParamChecker.notEmpty(queue, "queue");
      this.queue = queue;
      return this;
    }

    @Override
    public Reservation.Builder addResource(Resource resource) {
      ParamChecker.notNull(resource, "resource");
      this.resources.add(resource);
      return this;
    }

    @Override
    public Reservation.Builder addResources(List<Resource> resources) {
      ParamChecker.notNulls(resources, "resources");
      this.resources.addAll(resources);
      return this;
    }

    @Override
    public Reservation.Builder setResources(List<Resource> resources) {
      ParamChecker.notNulls(resources, "resources");
      this.resources.clear();
      this.resources.addAll(resources);
      return this;
    }

    @Override
    public Reservation.Builder setGang(boolean gang) {
      this.gang = gang;
      return this;
    }

    @Override
    public List<Resource> getResources() {
      return resources;
    }

    @Override
    public Reservation build() {
      ParamChecker.notNull(handle, "handle");
      ParamChecker.notNull(user, "user");
      ParamChecker.notEmpty(queue, "queue");
      ParamChecker.asserts(!resources.isEmpty(),
          "there must be at least one resource");
      return new PlacedReservationImpl(this);
    }

  }

  public static class XExpansionBuilder extends PlacedReservationImpl
      implements Expansion.Builder {
    private Resource resource;

    public XExpansionBuilder() {
    }

    @Override
    public Expansion.Builder setHandle(UUID handle) {
      ParamChecker.notNull(handle, "handle");
      this.handle = handle;
      return this;
    }

    @Override
    public Expansion.Builder setExpansionOf(UUID expansionOf) {
      ParamChecker.notNull(expansionOf, "expansionOf");
      this.expansionOf = expansionOf;
      return this;
    }

    @Override
    public Expansion.Builder setResource(Resource resource) {
      ParamChecker.notNull(resource, "resource");
      this.resource = resource;
      return this;
    }

    @Override
    public List<Resource> getResources() {
      return Arrays.asList(resource);
    }

    @Override
    public Expansion build() {
      ParamChecker.notNull(handle, "handle");
      ParamChecker.notNull(expansionOf, "expansionOf");
      ParamChecker.notNull(resource, "resource");
      return new PlacedReservationImpl(this);
    }
  }

  public static Reservation createReservationForExpansion(
      PlacedReservation originalReservation,  Expansion expansion) {
    List<PlacedResourceImpl> resources =new ArrayList<PlacedResourceImpl>();
    PlacedReservation reservation = new PlacedReservationImpl(null, null, 0,
        expansion.getHandle(), originalReservation.getUser(),
        originalReservation.getQueue(), false, resources,
        originalReservation.getReservationId(), 0, false);
    resources.add(PlacedResourceImpl.createPlaced(reservation,
        expansion.getResource()));
    return reservation;
  }

}

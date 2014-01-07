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

import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.api.Resource;
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.util.ParamChecker;
import com.cloudera.llama.util.UUID;

import java.util.HashMap;
import java.util.Map;

public class PlacedResourceImpl
    implements PlacedResource, RMResource {
  protected UUID resourceId;
  protected Status status;
  protected String locationAsk;
  protected Locality localityAsk;
  protected int cpuVCoresAsk;
  protected int memoryMbsAsk;
  protected long placedOn;
  protected UUID reservationId;
  protected UUID handle;
  protected String user;
  protected String queue;
  protected long allocatedOn;
  protected String location;
  protected int cpuVCores;
  protected int memoryMbs;
  protected Object rmResourceId;
  private Map<String, Object> rmData;

  public PlacedResourceImpl() {
  }

  PlacedResourceImpl(
      UUID resourceId,
      Status status,
      String locationAsk,
      Locality localityAsk,
      int cpuVCoresAsk,
      int memoryMbsAsk,
      long placedOn,
      UUID reservationId,
      UUID handle,
      String user,
      String queue,
      long allocatedOn,
      String location,
      int cpuVCores,
      int memoryMbs,
      Object rmResourceId) {
    this();
    this.resourceId = resourceId;
    this.status = status;
    this.locationAsk = locationAsk;
    this.localityAsk = localityAsk;
    this.cpuVCoresAsk = cpuVCoresAsk;
    this.memoryMbsAsk = memoryMbsAsk;
    this.placedOn = placedOn;
    this.reservationId = reservationId;
    this.handle = handle;
    this.user = user;
    this.queue = queue;
    this.allocatedOn = allocatedOn;
    this.location = location;
    this.cpuVCores = cpuVCores;
    this.memoryMbs = memoryMbs;
    this.rmResourceId = rmResourceId;
  }

  @SuppressWarnings("unchecked")
  public PlacedResourceImpl(PlacedResource r) {
    this(r.getResourceId(),
        r.getStatus(),
        r.getLocationAsk(),
        r.getLocalityAsk(),
        r.getCpuVCoresAsk(),
        r.getMemoryMbsAsk(),
        r.getPlacedOn(),
        r.getReservationId(),
        r.getHandle(),
        r.getUser(),
        r.getQueue(),
        r.getAllocatedOn(),
        r.getLocation(),
        r.getCpuVCores(),
        r.getMemoryMbs(),
        r.getRmResourceId());
  }

  @SuppressWarnings("unchecked")
  public static PlacedResourceImpl createPlaced(PlacedReservation reservation,
      Resource resource) {
    return new PlacedResourceImpl(resource.getResourceId(), Status.PENDING,
        resource.getLocationAsk(), resource.getLocalityAsk(),
        resource.getCpuVCoresAsk(), resource.getMemoryMbsAsk(),
        reservation.getPlacedOn(), reservation.getReservationId(),
        reservation.getHandle(), reservation.getUser(), reservation.getQueue(),
        -1, null, -1, -1, null);
  }

  private static final String RESOURCE_TO_STRING = "Resource[resourceId:{} " +
      "locationAsk:{} localityAsk:{} cpuVCoresAsk:{} memoryMbsAsk:{}]";

  private static final String PLACED_RESOURCE_TO_STRING = "PlacedResource[" +
      "resourceId:{} status:{} locationAsk:{} localityAsk:{} cpuVCoresAsk:{} " +
      "memoryMbsAsk:{} placedOn:{} reservationId:{} handle:{} user:{} " +
      "queue:{} allocatedOn:{} location:{} cpuVCores:{} memoryMbs:{} " +
      "rmResourceId:{}]";

  @Override
  public String toString() {
    String str;
    if (getResourceId() == null) {
      str = FastFormat.format(RESOURCE_TO_STRING, getResourceId(),
          getLocationAsk(), getLocalityAsk(), getCpuVCoresAsk(),
          getMemoryMbsAsk());
    } else {
      str= FastFormat.format(PLACED_RESOURCE_TO_STRING, getResourceId(),
          getStatus(), getLocationAsk(), getLocalityAsk(), getCpuVCoresAsk(),
          getMemoryMbsAsk(), getPlacedOn(), getReservationId(), getHandle(),
          getUser(), getQueue(), getAllocatedOn(), getLocation(),
          getCpuVCores(), getMemoryMbs(), getRmResourceId());
    }
    return str;
  }

  @Override
  public boolean equals(Object obj) {
    boolean eq = false;
    if (obj instanceof PlacedResourceImpl) {
      eq = (this == obj) ||
           getResourceId().equals(((PlacedResource) obj).getResourceId());
    }
    return eq;
  }

  @Override
  public int hashCode() {
    return getResourceId().hashCode();
  }

  @Override
  public UUID getResourceId() {
    return resourceId;
  }

  @Override
  public Status getStatus() {
    return status;
  }

  @Override
  public String getLocationAsk() {
    return locationAsk;
  }

  @Override
  public Locality getLocalityAsk() {
    return localityAsk;
  }

  @Override
  public int getCpuVCoresAsk() {
    return cpuVCoresAsk;
  }

  @Override
  public int getMemoryMbsAsk() {
    return memoryMbsAsk;
  }

  @Override
  public long getPlacedOn() {
    return placedOn;
  }

  @Override
  public UUID getReservationId() {
    return reservationId;
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
  public long getAllocatedOn() {
    return allocatedOn;
  }

  @Override
  public String getLocation() {
    return location;
  }

  @Override
  public int getCpuVCores() {
    return cpuVCores;
  }

  @Override
  public int getMemoryMbs() {
    return memoryMbs;
  }

  @Override
  public Object getRmResourceId() {
    return rmResourceId;
  }

  @Override
  public void setRmResourceId(Object rmResourceId) {
    this.rmResourceId = rmResourceId;
  }

  @Override
  public synchronized Map<String, Object> getRmData() {
    if (rmData == null) {
      rmData = new HashMap<String, Object>();
    }
    return rmData;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public void setAllocationInfo(String location, int cpuVCores, int memoryMbs) {
    status = Status.ALLOCATED;
    this.allocatedOn = Clock.currentTimeMillis();
    this.location = location;
    this.cpuVCores = cpuVCores;
    this.memoryMbs = memoryMbs;
  }

  public static class XResourceBuilder extends PlacedResourceImpl
      implements Builder {

    public XResourceBuilder() {
    }

    @Override
    public Builder setResourceId(UUID resourceId) {
      ParamChecker.notNull(resourceId, "resourceId");
      this.resourceId = resourceId;
      return this;
    }

    @Override
    public Builder setLocationAsk(String locationAsk) {
      ParamChecker.notEmpty(locationAsk, "locationAsk");
      this.locationAsk = locationAsk;
      return this;
    }

    @Override
    public Builder setLocalityAsk(Locality localityAsk) {
      ParamChecker.notNull(localityAsk, "localityAsk");
      this.localityAsk = localityAsk;
      return this;
    }

    @Override
    public Builder setCpuVCoresAsk(int cpuVCoresAsk) {
      ParamChecker.greaterEqualZero(cpuVCoresAsk, "cpuVCoresAsk");
      this.cpuVCoresAsk = cpuVCoresAsk;
      return this;
    }

    @Override
    public Builder setMemoryMbsAsk(int memoryMbsAsk) {
      ParamChecker.greaterEqualZero(memoryMbsAsk, "memoryMbsAsk");
      this.memoryMbsAsk = memoryMbsAsk;
      return this;
    }

    @Override
    public Resource build() {
      ParamChecker.notNull(resourceId, "resourceId");
      ParamChecker.notEmpty(locationAsk, "locationAsk");
      ParamChecker.notNull(localityAsk, "localityAsk");
      ParamChecker.greaterEqualZero(cpuVCoresAsk, "cpuVCoresAsk");
      ParamChecker.greaterEqualZero(memoryMbsAsk, "memoryMbsAsk");
      ParamChecker.asserts((cpuVCoresAsk != 0 || memoryMbsAsk != 0),
          "cpuVCores or memoryMbs must be greater than zero");
      return new PlacedResourceImpl(this);
    }

  }

}

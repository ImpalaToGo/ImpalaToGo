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
package com.cloudera.llama.am.spi;

import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.util.FastFormat;

import com.cloudera.llama.util.UUID;

import java.util.Map;

public class RMEvent {
  private final UUID resourceId;
  private final Object rmResourceId;
  private final Map<String, Object> rmData;
  private final PlacedResource.Status status;
  private final int cpuVCores;
  private final int memoryMbs;
  private final String location;

  private RMEvent(UUID resourceId, Object rmResourceId,
      Map<String, Object> rmData, String location, int cpuVCores, int memoryMbs,
      PlacedResource.Status status) {
    this.resourceId = resourceId;
    this.rmResourceId = rmResourceId;
    this.rmData = rmData;
    this.status = status;
    this.location = location;
    this.cpuVCores = cpuVCores;
    this.memoryMbs = memoryMbs;
  }

  public static RMEvent createAllocationEvent(UUID resourceId,
      String location, int vCpuCores, int memoryMb, Object rmResourceId,
      Map<String, Object> rmData) {
    return new RMEvent(resourceId, rmResourceId, rmData, location, vCpuCores,
        memoryMb, PlacedResource.Status.ALLOCATED);
  }

  public static RMEvent createStatusChangeEvent(UUID resourceId,
      PlacedResource.Status status) {
    return new RMEvent(resourceId, null, null, null, -1, -1, status);
  }

  public UUID getResourceId() {
    return resourceId;
  }

  public Object getRmResourceId() {
    return rmResourceId;
  }

  public Map<String, Object> getRmData() {
    return rmData;
  }

  public PlacedResource.Status getStatus() {
    return status;
  }

  public int getCpuVCores() {
    return cpuVCores;
  }

  public int getMemoryMbs() {
    return memoryMbs;
  }

  public String getLocation() {
    return location;
  }

  private static final String TO_STRING_ALLOCATED_MSG = "rmEvent[" +
      "resourceId: {} status: {} cpuVCores: {} memoryMbs: {} location: {}]";

  private static final String TO_STRING_CHANGED_MSG = "rmEvent[" +
      "resourceId: {} status: {}]";

  public String toString() {
    String msg = (getStatus() == PlacedResource.Status.ALLOCATED)
                 ? TO_STRING_ALLOCATED_MSG : TO_STRING_CHANGED_MSG;
    return FastFormat.format(msg, getResourceId(), getStatus(),
        getCpuVCores(), getMemoryMbs(), getLocation());
  }

}

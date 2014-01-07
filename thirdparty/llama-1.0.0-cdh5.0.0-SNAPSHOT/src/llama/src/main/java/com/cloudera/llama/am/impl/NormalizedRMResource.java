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

import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.util.UUID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NormalizedRMResource implements RMResource {

  /**
   * Breaks a resource into fragments. Each fragment has only one resource type
   * (memory/cpu) and the size of that resource is the given stdCpuVCores or
   * given stdMemoryMbs.
   */
  public static List<NormalizedRMResource> normalize(RMResource rmResource,
      int stdCpuVCores, int stdMemoryMbs) {
    int cpuResources = getNormalizedCount(rmResource.getCpuVCoresAsk(),
        stdCpuVCores);
    int memoryResources = getNormalizedCount(rmResource.getMemoryMbsAsk(),
        stdMemoryMbs);
    List<NormalizedRMResource> list = new ArrayList<NormalizedRMResource>();
    for (int i = 0; i < cpuResources; i++) {
      list.add(new NormalizedRMResource(rmResource, stdCpuVCores, 0));
    }
    for (int i = 0; i < memoryResources; i++) {
      list.add(new NormalizedRMResource(rmResource, 0, stdMemoryMbs));
    }
    return list;
  }

  private static int getNormalizedCount(int requested, int normal) {
    return requested / normal + ((requested % normal == 0) ? 0 : 1);
  }

  private final RMResource source;
  private final int cpuVCoresAsk;
  private final int memoryMbsAsk;
  private String location;
  private int cpuVCores;
  private int memoryMbs;
  private UUID resourceId;
  private Object rmResourceId;
  private final Map<String, Object> rmData;

  public NormalizedRMResource(RMResource source, int effectiveCpuVCores,
      int effectiveMemoryMb) {
    this.source = source;
    cpuVCoresAsk = effectiveCpuVCores;
    memoryMbsAsk = effectiveMemoryMb;
    resourceId = UUID.randomUUID();
    rmData = new HashMap<String, Object>();
  }

  public RMResource getSource() {
    return source;
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
  public UUID getResourceId() {
    return resourceId;
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
  public Map<String, Object> getRmData() {
    return rmData;
  }

  @Override
  public String getLocationAsk() {
    return source.getLocationAsk();
  }

  @Override
  public Locality getLocalityAsk() {
    return Locality.MUST;
  }

  @Override
  public int getCpuVCoresAsk() {
    return cpuVCoresAsk;
  }

  @Override
  public int getMemoryMbsAsk() {
    return memoryMbsAsk;
  }

  public void setAllocationInfo(String location, int cpuVCores,
      int memoryMbs) {
    this.location = location;
    this.cpuVCores = cpuVCores;
    this.memoryMbs = memoryMbs;
  }

  private static final String TO_STRING = "NormalizedRmResource[" +
      "sourceResourceId:{} resourceId:{} locationAsk:{} localityAsk:{} " +
      "cpuVCoresAsk:{} memoryMbsAsk:{} location:{} cpuVCores:{} memoryMbs:{} " +
      "rmResourceId:{}]";

  @Override
  public String toString() {
    return FastFormat.format(TO_STRING, getSource().getResourceId(),
        getResourceId(), getLocationAsk(), getLocalityAsk(), getCpuVCoresAsk(),
        getMemoryMbsAsk(), getLocation(), getCpuVCores(), getMemoryMbs(),
        getRmResourceId());
  }
}

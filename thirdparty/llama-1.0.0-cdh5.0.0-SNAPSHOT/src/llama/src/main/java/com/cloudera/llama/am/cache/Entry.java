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
package com.cloudera.llama.am.cache;

import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.spi.RMEvent;
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.util.UUID;

import java.util.Map;

public class Entry implements Comparable<Entry>, CacheRMResource {
  private final UUID id;
  private final long cachedOn;
  private final Object rmResourceId;
  private final String location;
  private final int cpuVCores;
  private final int memoryMbs;
  private final Map<String, Object> rmData;
  private volatile boolean valid;

  public static Entry createStoreEntry(RMResource resource) {
    return new Entry(resource.getResourceId(), resource.getRmResourceId(),
        resource.getRmData(), resource.getLocationAsk(),
        resource.getCpuVCoresAsk(), resource.getMemoryMbsAsk());
  }

  public static Entry createCacheEntry(RMResource resource) {
    return new Entry(UUID.randomUUID(), resource.getRmResourceId(),
        resource.getRmData(), resource.getLocation(), resource.getCpuVCores(),
        resource.getMemoryMbs());
  }

  public static Entry createCacheEntry(RMEvent rmEvent) {
    return new Entry(UUID.randomUUID(), rmEvent.getRmResourceId(),
        rmEvent.getRmData(), rmEvent.getLocation(), rmEvent.getCpuVCores(),
        rmEvent.getMemoryMbs());
  }

  // used internally by ResourceStore
  Entry(String location, int cpuVCores, int memoryMbs) {
    id = null;
    cachedOn = 0;
    rmResourceId = null;
    this.location = location;
    this.cpuVCores = cpuVCores;
    this.memoryMbs = memoryMbs;
    rmData = null;
  }

  private Entry(UUID id, Object rmResourceId, Map<String, Object> rmData,
      String location, int cpuVCores, int memoryMbs) {
    this.id = id;
    this.cachedOn = Clock.currentTimeMillis();
    this.rmResourceId = rmResourceId;
    this.location = location;
    this.cpuVCores = cpuVCores;
    this.memoryMbs = memoryMbs;
    this.rmData = rmData;
  }

  void setValid(boolean valid) {
    this.valid = valid;
  }

  @Override
  public int compareTo(Entry o) {
    return getLocation().compareTo(o.getLocation());
  }

  @Override
  public UUID getResourceId() {
    return id;
  }

  @Override
  public long getCachedOn() {
    return cachedOn;
  }

  @Override
  public Object getRmResourceId() {
    return rmResourceId;
  }

  @Override
  public Locality getLocalityAsk() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getLocationAsk() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getCpuVCoresAsk() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMemoryMbsAsk() {
    throw new UnsupportedOperationException();
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
  public Map<String, Object> getRmData() {
    return rmData;
  }

  @Override
  public void setRmResourceId(Object rmResourceId) {
    throw new UnsupportedOperationException();
  }

  public boolean isValid() {
    return valid;
  }

  private static final String TO_STRING = "ResourceCache [id: {} " +
      "cachedOn: {} rmResourceId: {} location: {} cpuVCores: {} memoryMbs: {}]";

  @Override
  public String toString() {
    return FastFormat.format(TO_STRING, getResourceId(), getCachedOn(),
        getRmResourceId(), getLocation(), getCpuVCores(), getMemoryMbs());
  }

}

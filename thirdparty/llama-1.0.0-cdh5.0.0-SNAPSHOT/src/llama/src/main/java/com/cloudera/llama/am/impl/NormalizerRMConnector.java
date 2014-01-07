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
import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.spi.RMConnector;
import com.cloudera.llama.am.spi.RMEvent;
import com.cloudera.llama.am.spi.RMListener;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Breaks up resource requests into smaller requests of standardized size.  The
 * advantage of a standard size is that, when the normalizer wraps the cache,
 * requests can be satisfied by cached resources even when the original request
 * sizes were different.
 */
public class NormalizerRMConnector implements RMConnector, RMListener {
  private static final Logger LOG =
      LoggerFactory.getLogger(NormalizerRMConnector.class);

  private RMConnector connector;
  private RMListener listener;
  private int normalCpuVCores;
  private int normalMemoryMbs;

  private Map<UUID, ResourceEntry> normalizedToEntry;
  private Map<UUID, ResourceEntry> originalToEntry;

  public NormalizerRMConnector(Configuration conf, RMConnector connector) {
    this.connector = connector;
    connector.setLlamaAMCallback(this);
    normalCpuVCores = conf.getInt(LlamaAM.NORMALIZING_STANDARD_VCORES_KEY,
        LlamaAM.NORMALIZING_SIZE_VCORES_DEFAULT);
    normalMemoryMbs = conf.getInt(LlamaAM.NORMALIZING_STANDARD_MBS_KEY,
        LlamaAM.NORMALIZING_SIZE_MBS_DEFAULT);
    normalizedToEntry = new HashMap<UUID, ResourceEntry>();
    originalToEntry = new HashMap<UUID, ResourceEntry>();
  }

  @Override
  public void setMetricRegistry(MetricRegistry metricRegistry) {
    connector.setMetricRegistry(metricRegistry);
  }

  @Override
  public void setLlamaAMCallback(RMListener callback) {
    this.listener = callback;
  }

  @Override
  public void start() throws LlamaException {
    connector.start();
  }

  @Override
  public void stop() {
    connector.stop();
  }

  @Override
  public void register(String queue) throws LlamaException {
    connector.register(queue);
  }

  @Override
  public void unregister() {
    connector.unregister();
  }

  @Override
  public List<String> getNodes() throws LlamaException {
    return connector.getNodes();
  }

  private static class ResourceEntry {
    private RMResource original;
    private List<NormalizedRMResource> normalized;
    private int waitingAllocations;
    private String location;
    private int cpuVCores;
    private int memoryMbs;

    public ResourceEntry(RMResource original, List<NormalizedRMResource> normalized) {
      this.original = original;
      this.normalized = normalized;
      waitingAllocations = normalized.size();
    }

    public RMResource getOriginal() {
      return original;
    }

    public List<NormalizedRMResource> getNormalized() {
      return normalized;
    }

    /**
     * Returns true if, after adding this allocation, the original resource
     * request is fully satisfied.
     */
    public boolean addAllocation(UUID normalizedId, String location,
        int cpuVCores, int memoryMbs) {
      boolean fullyAllocated = false;
      NormalizedRMResource nr = null;
      for (int i = 0; nr == null && i < normalized.size(); i++) {
        if (normalized.get(i).getResourceId().equals(normalizedId)) {
          nr = normalized.get(i);
        }
      }
      if (nr != null) {
        synchronized (this) {
          this.location = location;
          this.cpuVCores += cpuVCores;
          this.memoryMbs += memoryMbs;
          nr.setAllocationInfo(location, cpuVCores, memoryMbs);
          waitingAllocations--;
          fullyAllocated = waitingAllocations == 0;
        }
      }
      return fullyAllocated;
    }

    public Object getRmResourceId() {
      List<Object> list = new ArrayList<Object>();
      for (NormalizedRMResource nr : getNormalized()) {
        list.add(nr.getRmResourceId());
      }
      return list;
    }

    public RMEvent createAllocatedEvent() {
      return RMEvent.createAllocationEvent(original.getResourceId(), location,
          cpuVCores, memoryMbs, getRmResourceId(), null);
    }

  }

  synchronized ResourceEntry addEntry(RMResource original,
      List<NormalizedRMResource> normalized) {
    ResourceEntry entry = new ResourceEntry(original, normalized);
    originalToEntry.put(original.getResourceId(), entry);
    for (NormalizedRMResource nr : normalized) {
      normalizedToEntry.put(nr.getResourceId(), entry);
    }
    return entry;
  }

  synchronized ResourceEntry removeEntry(UUID originalId) {
    ResourceEntry entry = originalToEntry.remove(originalId);
    if (entry != null) {
      for (NormalizedRMResource nr : entry.getNormalized()) {
        if (normalizedToEntry.remove(nr.getResourceId()) == null) {
          LOG.warn("On removal, expected entry for normalized resource '{}'" +
              " with original ID '{}'", nr.getResourceId(), originalId);
        }
      }
    }
    return entry;
  }

  synchronized ResourceEntry getEntryUsingOriginalId(UUID id) {
    return originalToEntry.get(id);
  }

  synchronized ResourceEntry getEntryUsingNormalizedId(UUID id) {
    return normalizedToEntry.get(id);
  }

  @Override
  public void reserve(Collection<RMResource> resources) throws LlamaException {
    List<RMResource> normalizedResources = new ArrayList<RMResource>();
    for (RMResource resource : resources) {
      List<NormalizedRMResource> normalize = NormalizedRMResource.normalize(
          resource, normalCpuVCores, normalMemoryMbs);
      addEntry(resource, normalize);
      normalizedResources.addAll(normalize);
      LOG.debug("Split resource ask with '{}' MBs and '{}' vcores into '{}' " +
          "normalized resources", resource.getMemoryMbsAsk(),
      		resource.getCpuVCoresAsk(), normalize.size());
    }
    try {
      connector.reserve(normalizedResources);
    } catch (LlamaException ex) {
      for (RMResource resource : resources) {
        removeEntry(resource.getResourceId());
      }
      throw ex;
    }
  }

  @Override
  public void release(Collection<RMResource> resources, boolean doNotCache)
      throws LlamaException {
    List<RMResource> toRemove = new ArrayList<RMResource>();
    for (RMResource resource : resources) {
      ResourceEntry entry = removeEntry(resource.getResourceId());
      if (entry != null) {
        toRemove.addAll(entry.getNormalized());
        LOG.debug("Releasing '{}' normalized chunks for resource '{}'",
            entry.getNormalized().size(), resource.getResourceId());
      }
    }
    connector.release(toRemove, doNotCache);
  }

  @Override
  public boolean reassignResource(Object rmResourceId, UUID resourceId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void emptyCache() throws LlamaException {
    connector.emptyCache();
  }


  @Override
  public void stoppedByRM() {
    listener.stoppedByRM();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void onEvent(List<RMEvent> events) {
    List<RMEvent> normalizerEvents = new ArrayList<RMEvent>();
    for (RMEvent event : events) {
      ResourceEntry entry = getEntryUsingNormalizedId(event.getResourceId());
      // When a normalized resource is rejected/preempted/whatever we release
      // all normalized resources corresponding to the same original resource.
      // If this happens for multiple normalized resources corresponding to the
      // same original resource in a single call to this function, entry will
      // be null for the all the events after the first.
      if (entry != null) {
        switch (event.getStatus()) {
          case ALLOCATED:
            if (entry.addAllocation(event.getResourceId(), event.getLocation(),
                event.getCpuVCores(), event.getMemoryMbs())) {
              RMResource original = entry.getOriginal();
              LOG.debug("All normalized chunks allocated for resource '{}'",
                  original.getResourceId());
              original.setRmResourceId(entry.getRmResourceId());
              normalizerEvents.add(entry.createAllocatedEvent());
            }
            break;
          case PENDING:
            LOG.error("Normalizer should not receive PENDING event");
          case REJECTED:
          case PREEMPTED:
          case LOST:
          case RELEASED:
            UUID originalId = entry.getOriginal().getResourceId();
            removeEntry(originalId);
            normalizerEvents.add(RMEvent.createStatusChangeEvent(originalId,
                event.getStatus()));
            try {
              connector.release((List<RMResource>) (List) entry.getNormalized(), true);
            } catch (LlamaException ex) {
              LOG.warn("Exception while releasing entry", ex);
            }
            break;
        }
      }
    }
    if (!normalizerEvents.isEmpty()) {
      listener.onEvent(normalizerEvents);
    }
  }

}

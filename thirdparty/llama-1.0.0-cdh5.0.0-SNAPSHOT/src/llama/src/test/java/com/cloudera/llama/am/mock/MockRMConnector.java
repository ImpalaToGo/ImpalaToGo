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
package com.cloudera.llama.am.mock;

import com.cloudera.llama.am.api.LlamaAM;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.spi.RMEvent;
import com.cloudera.llama.am.spi.RMListener;
import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.am.spi.RMConnector;
import com.cloudera.llama.util.NamedThreadFactory;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

// if CPU value is greater than 10, that is the number of milliseconds for the
// change status delay (instead being random)
public class MockRMConnector
    implements RMConnector, Configurable {
  public static final String PREFIX_KEY = LlamaAM.PREFIX_KEY + "mock.";

  public static final String EVENTS_MIN_WAIT_KEY =
      MockRMConnector.PREFIX_KEY + "events.min.wait.ms";
  public static final int EVENTS_MIN_WAIT_DEFAULT = 1000;

  public static final String EVENTS_MAX_WAIT_KEY =
      MockRMConnector.PREFIX_KEY + "events.max.wait.ms";
  public static final int EVENTS_MAX_WAIT_DEFAULT = 10000;

  public static final String QUEUES_KEY = MockRMConnector.PREFIX_KEY +
      "queues";
  public static final Set<String> QUEUES_DEFAULT = new HashSet<String>();

  static {
    QUEUES_DEFAULT.add("queue1");
    QUEUES_DEFAULT.add("queue2");
  }

  public static final String NODES_KEY = MockRMConnector.PREFIX_KEY +
      "nodes";
  public static final String NODES_DEFAULT = "node1,node2";

  private static final Map<String, PlacedResource.Status> MOCK_FLAGS = new
      HashMap<String, PlacedResource.Status>();

  static {
    MOCK_FLAGS.put(MockLlamaAMFlags.ALLOCATE, PlacedResource.Status.ALLOCATED);
    MOCK_FLAGS.put(MockLlamaAMFlags.REJECT, PlacedResource.Status.REJECTED);
    MOCK_FLAGS.put(MockLlamaAMFlags.LOSE, PlacedResource.Status.LOST);
    MOCK_FLAGS.put(MockLlamaAMFlags.PREEMPT, PlacedResource.Status.PREEMPTED);
    MOCK_FLAGS.put(MockLlamaAMFlags.PENDING, PlacedResource.Status.PENDING);
  }

  private static final Random RANDOM = new Random();

  private PlacedResource.Status getMockResourceStatus(String location) {
    PlacedResource.Status status = null;
    for (Map.Entry<String, PlacedResource.Status> entry : MOCK_FLAGS.entrySet
        ()) {
      if (location.startsWith(entry.getKey())) {
        status = entry.getValue();
      }
    }
    if (!nodes.contains(getLocation(location))) {
      status = PlacedResource.Status.REJECTED;
    } else if (status == null) {
      int r = RANDOM.nextInt(10);
      if (r < 1) {
        status = PlacedResource.Status.LOST;
      } else if (r < 2) {
        status = PlacedResource.Status.REJECTED;
      } else if (r < 4) {
        status = PlacedResource.Status.PREEMPTED;
      } else {
        status = PlacedResource.Status.ALLOCATED;
      }
    }
    return status;
  }

  private String getLocation(String location) {
    for (Map.Entry<String, PlacedResource.Status> entry : MOCK_FLAGS.entrySet
        ()) {
      if (location.startsWith(entry.getKey())) {
        return location.substring(entry.getKey().length());
      }
    }
    return location;
  }

  private final AtomicLong counter = new AtomicLong();
  private Configuration conf;
  private ScheduledExecutorService scheduler;
  private int minWait;
  private int maxWait;
  private List<String> nodes;
  private RMListener callback;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setLlamaAMCallback(RMListener callback) {
    this.callback = callback;
  }

  @Override
  public void start() throws LlamaException {
  }

  @Override
  public void stop() {
  }

  @Override
  public void register(String queue) throws LlamaException {
    minWait = getConf().getInt(EVENTS_MIN_WAIT_KEY, EVENTS_MIN_WAIT_DEFAULT);
    maxWait = getConf().getInt(EVENTS_MAX_WAIT_KEY, EVENTS_MAX_WAIT_DEFAULT);
    nodes = Collections.unmodifiableList(Arrays.asList(getConf().
        getStrings(NODES_KEY, NODES_DEFAULT)));
    scheduler = new ScheduledThreadPoolExecutor(1,
        new NamedThreadFactory("llama-mock"));
    if (queue != null) {
      Collection<String> validQueues = getConf().
          getTrimmedStringCollection(QUEUES_KEY);
      if (validQueues.isEmpty()) {
        validQueues = QUEUES_DEFAULT;
      }
      if (!validQueues.contains(queue)) {
        throw new IllegalArgumentException(FastFormat.format("Invalid queue " +
            "'{}'", queue));
      }
    }
  }

  @Override
  public void unregister() {
    scheduler.shutdownNow();
  }

  @Override
  public List<String> getNodes() throws LlamaException {
    return Collections.unmodifiableList(Arrays.asList(getConf().
        getStrings(NODES_KEY, NODES_DEFAULT)));
  }

  @Override
  public void reserve(Collection<RMResource> resources)
      throws LlamaException {
    schedule(this, resources);
  }

  @Override
  public void release(Collection<RMResource> resources, boolean doNotCache)
      throws LlamaException {
  }

  @Override
  public void emptyCache() {
  }

  @Override
  public boolean reassignResource(Object rmResourceId, UUID resourceId) {
    return false;
  }

  private class MockRMAllocator implements Callable<Void> {
    private MockRMConnector llama;
    private RMResource resource;
    private PlacedResource.Status status;
    private boolean initial;

    public MockRMAllocator(MockRMConnector llama,
        RMResource resource,
        PlacedResource.Status status, boolean initial) {
      this.llama = llama;
      this.resource = resource;
      this.status = status;
      this.initial = initial;
    }

    private void toAllocate() {
      RMEvent change = RMEvent.createAllocationEvent(resource.getResourceId(),
          getLocation(resource.getLocationAsk()), resource.getCpuVCoresAsk(),
          resource.getMemoryMbsAsk(), "c" + counter.incrementAndGet(),
          new HashMap<String, Object>());
      callback.onEvent(Arrays.asList(change));
    }

    private void toStatus(PlacedResource.Status status) {
      RMEvent change = RMEvent.createStatusChangeEvent(
          resource.getResourceId(), status);
      callback.onEvent(Arrays.asList(change));

    }

    @Override
    public Void call() throws Exception {
      switch (status) {
        case ALLOCATED:
          toAllocate();
          break;
        case REJECTED:
          toStatus(status);
          break;
        case LOST:
        case PREEMPTED:
          if (initial) {
            toAllocate();

            MockRMAllocator mocker = new MockRMAllocator(llama, resource,
                status, false);
            int delay;
            if (resource.getCpuVCores()  > 10) {
              delay = resource.getCpuVCores();
            } else {
              delay = minWait + RANDOM.nextInt(maxWait);
            }
            scheduler.schedule(mocker, delay, TimeUnit.MILLISECONDS);
          } else {
            toStatus(status);
          }
          break;
        case PENDING:
          break;
      }
      return null;
    }
  }

  private void schedule(MockRMConnector allocator,
      Collection<RMResource> resources) {
    for (RMResource resource : resources) {
      PlacedResource.Status status = getMockResourceStatus(
          resource.getLocationAsk());
      MockRMAllocator mocker = new MockRMAllocator(allocator, resource, status,
          true);
      int delay = minWait + RANDOM.nextInt(maxWait);
      scheduler.schedule(mocker, delay, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void setMetricRegistry(MetricRegistry registry) {
  }
}

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

import com.cloudera.llama.am.impl.PlacedResourceImpl;
import com.cloudera.llama.am.impl.RecordingMockRMConnector;
import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.api.Resource;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.am.spi.RMEvent;
import com.cloudera.llama.am.spi.RMListener;
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.ManualClock;
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class TestCacheRMConnector {
  private ManualClock manualClock = new ManualClock();

  @Before
  public void setup() {
    Clock.setClock(manualClock);
  }

  @After
  public void destroy() {
    Clock.setClock(Clock.SYSTEM);
  }

  @Test
  public void testDelegation() throws Exception {
    List<String> expected = new ArrayList<String>();
    expected.add("setLlamaAMCallback");

    RecordingMockRMConnector connector = new RecordingMockRMConnector();

    CacheRMConnector cache = new CacheRMConnector(
        new Configuration(false), connector);

    Assert.assertEquals(expected, connector.getInvoked());

    expected.add("start");
    expected.add("getNodes");
    expected.add("register");

    cache.setLlamaAMCallback(new RMListener() {
      @Override
      public void stoppedByRM() {
      }

      @Override
      public void onEvent(List<RMEvent> events) {
      }
    });
    cache.start();
    cache.getNodes();
    cache.register("q");
    cache.reassignResource("rm0", UUID.randomUUID());

    Assert.assertEquals(expected, connector.getInvoked());

    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl("l1",
        Resource.Locality.MUST, 1, 1024);


    cache.reserve(Arrays.asList((RMResource)pr1));
    pr1.setAllocationInfo("'l1", 1, 1024);
    pr1.setRmResourceId("rm1");

    expected.add("reserve");
    Assert.assertEquals(expected, connector.getInvoked());

    cache.release(Arrays.asList((RMResource)pr1), false);

    expected.add("release");
    expected.add("reassignResource");
    Assert.assertEquals(expected, connector.getInvoked());

    cache.release(Arrays.asList((RMResource) pr1), true);

    expected.add("release");
    Assert.assertEquals(expected, connector.getInvoked());

    expected.add("unregister");
    expected.add("stop");

    cache.unregister();
    cache.stop();
    Assert.assertEquals(expected, connector.getInvoked());
  }

  @Test
  public void testCached() throws Exception {
    RecordingMockRMConnector connector = new RecordingMockRMConnector();
    final List<RMEvent> rmEvents = new ArrayList<RMEvent>();

    CacheRMConnector cache = new CacheRMConnector(
        new Configuration(false), connector);

    cache.setLlamaAMCallback(new RMListener() {
      @Override
      public void stoppedByRM() {
      }

      @Override
      public void onEvent(List<RMEvent> events) {
        rmEvents.addAll(events);
      }
    });

    cache.start();
    cache.getNodes();
    cache.register("q");

    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl("l1",
        Resource.Locality.MUST, 1, 1024);

    cache.reserve(Arrays.asList((RMResource) pr1));

    Assert.assertTrue(connector.getInvoked().contains("reserve"));
    Assert.assertTrue(rmEvents.isEmpty());

    cache.onEvent(Arrays.asList(RMEvent.createAllocationEvent(
        pr1.getResourceId(), "l1", 1, 1024, "rm1",
        new HashMap<String, Object>())));

    pr1.setAllocationInfo("l1", 1, 1024);
    pr1.setRmResourceId("rm1");

    cache.release(Arrays.asList((RMResource) pr1), false);

    rmEvents.clear();
    connector.getInvoked().clear();

    PlacedResourceImpl pr2 = TestUtils.createPlacedResourceImpl("l1",
        Resource.Locality.MUST, 1, 1024);

    cache.reserve(Arrays.asList((RMResource) pr2));

    Assert.assertFalse(connector.getInvoked().contains("reserve"));
    Assert.assertFalse(rmEvents.isEmpty());
    Assert.assertEquals(pr2.getResourceId(), rmEvents.get(0).getResourceId());
    cache.unregister();
    cache.stop();
  }

  @Test
  public void testDoNotCache() throws Exception {
    RecordingMockRMConnector connector = new RecordingMockRMConnector();

    CacheRMConnector cache = new CacheRMConnector(
        new Configuration(false), connector);

    cache.setLlamaAMCallback(new RMListener() {
      @Override
      public void stoppedByRM() {
      }

      @Override
      public void onEvent(List<RMEvent> events) {
      }
    });

    cache.start();
    cache.getNodes();
    cache.register("q");

    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl("l1",
        Resource.Locality.MUST, 1, 1024);


    cache.reserve(Arrays.asList((RMResource) pr1));
    pr1.setAllocationInfo("'l1", 1, 1024);
    pr1.setRmResourceId("rm1");

    cache.onEvent(Arrays.asList(RMEvent.createAllocationEvent(
        pr1.getResourceId(), "l1", 1, 1024, "rm1",
        new HashMap<String, Object>())));

    cache.release(Arrays.asList((RMResource) pr1), false);

    Assert.assertFalse(connector.getInvoked().contains("release"));

    cache.release(Arrays.asList((RMResource) pr1), true);

    Assert.assertTrue(connector.getInvoked().contains("release"));

    cache.unregister();
    cache.stop();
  }

  @Test
  public void testEviction() throws Exception {
    RecordingMockRMConnector connector = new RecordingMockRMConnector();

    CacheRMConnector cache = new CacheRMConnector(
        new Configuration(false), connector);

    cache.setLlamaAMCallback(new RMListener() {
      @Override
      public void stoppedByRM() {
      }

      @Override
      public void onEvent(List<RMEvent> events) {
      }
    });

    cache.start();
    cache.getNodes();
    cache.register("q");

    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl("l1",
        Resource.Locality.MUST, 1, 1024);


    cache.reserve(Arrays.asList((RMResource) pr1));
    pr1.setAllocationInfo("'l1", 1, 1024);
    pr1.setRmResourceId("rm1");

    cache.onEvent(Arrays.asList(RMEvent.createAllocationEvent(
        pr1.getResourceId(), "l1", 1, 1024, "rm1",
        new HashMap<String, Object>())));

    cache.release(Arrays.asList((RMResource) pr1), false);
    Assert.assertFalse(connector.getInvoked().contains("release"));

    manualClock.increment(ResourceCache.EVICTION_IDLE_TIMEOUT_DEFAULT+1);
    Thread.sleep(100);

    Assert.assertTrue(connector.getInvoked().contains("release"));

    cache.unregister();
    cache.stop();
  }

  @Test
  public void testEmptyCache() throws Exception {
    RecordingMockRMConnector connector = new RecordingMockRMConnector();

    CacheRMConnector cache = new CacheRMConnector(
        new Configuration(false), connector);

    cache.setLlamaAMCallback(new RMListener() {
      @Override
      public void stoppedByRM() {
      }

      @Override
      public void onEvent(List<RMEvent> events) {
      }
    });

    cache.start();
    cache.getNodes();
    cache.register("q");

    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl("l1",
        Resource.Locality.MUST, 1, 1024);


    cache.reserve(Arrays.asList((RMResource) pr1));
    pr1.setAllocationInfo("'l1", 1, 1024);
    pr1.setRmResourceId("rm1");

    cache.onEvent(Arrays.asList(RMEvent.createAllocationEvent(
        pr1.getResourceId(), "l1", 1, 1024, "rm1",
        new HashMap<String, Object>())));

    cache.release(Arrays.asList((RMResource) pr1), false);
    Assert.assertFalse(connector.getInvoked().contains("release"));

    cache.emptyCache();

    Assert.assertTrue(connector.getInvoked().contains("release"));

    cache.unregister();
    cache.stop();
  }

  @Test
  public void testMatchingOnRelease() throws Exception {
    RecordingMockRMConnector connector = new RecordingMockRMConnector();
    final List<RMEvent> rmEvents = new ArrayList<RMEvent>();

    CacheRMConnector cache = new CacheRMConnector(
        new Configuration(false), connector);

    cache.setLlamaAMCallback(new RMListener() {
      @Override
      public void stoppedByRM() {
      }

      @Override
      public void onEvent(List<RMEvent> events) {
        rmEvents.addAll(events);
      }
    });

    cache.start();
    cache.getNodes();
    cache.register("q");

    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl("l1",
        Resource.Locality.MUST, 1, 1024);
    cache.reserve(Arrays.asList((RMResource) pr1));

    Assert.assertTrue(connector.getInvoked().contains("reserve"));
    Assert.assertTrue(rmEvents.isEmpty());

    Assert.assertEquals(1, cache.getPendingSize());
    Assert.assertEquals(0, cache.getCacheSize());

    cache.onEvent(Arrays.asList(RMEvent.createAllocationEvent(
        pr1.getResourceId(), "l1", 1, 1024, "rm1",
        new HashMap<String, Object>())));

    Assert.assertEquals(0, cache.getPendingSize());
    Assert.assertEquals(0, cache.getCacheSize());

    Assert.assertEquals(1, rmEvents.size());
    Assert.assertEquals(pr1.getResourceId(), rmEvents.get(0).getResourceId());
    rmEvents.clear();

    pr1.setAllocationInfo("l1", 1, 1024);
    pr1.setRmResourceId("rm1");

    PlacedResourceImpl pr2 = TestUtils.createPlacedResourceImpl("l1",
        Resource.Locality.MUST, 1, 1024);
    cache.reserve(Arrays.asList((RMResource) pr2));

    Assert.assertEquals(1, cache.getPendingSize());
    Assert.assertEquals(0, cache.getCacheSize());

    cache.release(Arrays.asList((RMResource) pr1), false);

    Assert.assertEquals(0, cache.getPendingSize());
    Assert.assertEquals(0, cache.getCacheSize());

    Assert.assertEquals(1, rmEvents.size());
    Assert.assertEquals(pr2.getResourceId(), rmEvents.get(0).getResourceId());


    cache.unregister();
    cache.stop();
  }


}

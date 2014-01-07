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

import com.cloudera.llama.am.api.Resource;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.am.impl.PlacedResourceImpl;
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.ManualClock;
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestResourceCache {
  private ManualClock manualClock = new ManualClock();

  @Before
  public void setup() {
    Clock.setClock(manualClock);
  }

  @After
  public void destroy() {
    Clock.setClock(Clock.SYSTEM);
  }

  private void testTimeoutEvictionPolicy(long timeout) throws Exception {
    ResourceCache.TimeoutEvictionPolicy ep =
        new ResourceCache.TimeoutEvictionPolicy();

    Configuration conf = new Configuration(false);
    if (timeout > 0) {
      conf.setLong(ResourceCache.EVICTION_IDLE_TIMEOUT_KEY, timeout);
    }
    ep.setConf(conf);

    long expected = (timeout == 0)
                    ? ResourceCache.EVICTION_IDLE_TIMEOUT_DEFAULT : timeout;
    Assert.assertEquals(expected, ep.getTimeout());

    manualClock.set(1000);
    CacheRMResource cr =
        Mockito.mock(CacheRMResource.class);
    Mockito.when(cr.getCachedOn()).thenReturn(1000l);
    Assert.assertFalse(ep.shouldEvict(cr));
    manualClock.increment(ep.getTimeout() - 1);
    Assert.assertFalse(ep.shouldEvict(cr));
    manualClock.increment(1);
    Assert.assertTrue(ep.shouldEvict(cr));
    manualClock.increment(1);
    Assert.assertTrue(ep.shouldEvict(cr));
  }

  @Test
  public void testDefaultTimeoutEvictionPolicy() throws Exception {
    testTimeoutEvictionPolicy(0);
  }

  @Test
  public void testCustomTimeoutEvictionPolicy() throws Exception {
    testTimeoutEvictionPolicy(10);
  }

  private static class CacheListener implements ResourceCache.Listener {
    Object resourceEvicted;

    @Override
    public void onEviction(CacheRMResource cachedRMResource) {
      resourceEvicted = cachedRMResource.getRmResourceId();
    }
  }

  @Test
  public void testCacheStartStop() throws Exception {
    CacheListener listener = new CacheListener();
    ResourceCache cache = new ResourceCache("q", new Configuration(false), listener);
    try {
      cache.start();
    } finally {
      cache.stop();
    }
  }

  @Test
  public void testCacheEviction() throws Exception {
    CacheListener listener = new CacheListener();
    ResourceCache cache = new ResourceCache("q", new Configuration(false),
        listener);
    try {
      cache.start();
      manualClock.increment(ResourceCache.EVICTION_IDLE_TIMEOUT_DEFAULT + 1);
      Thread.sleep(100); //to ensure eviction thread runs
      Resource r1 = TestUtils.createResource("l1",
          Resource.Locality.MUST, 1, 1024);
      PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl(r1);
      pr1.setAllocationInfo("l1", 1, 1024);
      pr1.setRmResourceId("rm1");
      cache.add(Entry.createCacheEntry(pr1));
      Assert.assertNull(listener.resourceEvicted);
      manualClock.increment(ResourceCache.EVICTION_IDLE_TIMEOUT_DEFAULT + 1);
      cache.runEviction();
      Assert.assertEquals("rm1", listener.resourceEvicted);
    } finally {
      cache.stop();
    }
  }

  @Test
  public void testCacheSize() throws Exception {
    CacheListener listener = new CacheListener();
    ResourceCache cache = new ResourceCache("q", new Configuration(false),
        listener);
    try {
      cache.start();

      Assert.assertEquals(0, cache.getSize());

      Resource r1 = TestUtils.createResource("l1",
          Resource.Locality.MUST, 1, 1024);
      PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl(r1);
      pr1.setAllocationInfo("l1", 1, 1024);
      pr1.setRmResourceId("rm1");
      cache.add(Entry.createCacheEntry(pr1));

      Assert.assertNull(listener.resourceEvicted);
      Assert.assertEquals(1, cache.getSize());

      manualClock.increment(ResourceCache.EVICTION_IDLE_TIMEOUT_DEFAULT / 2 + 1);

      Assert.assertNull(listener.resourceEvicted);
      Assert.assertEquals(1, cache.getSize());

      pr1.setAllocationInfo("l1", 1, 1024);
      pr1.setRmResourceId("rm2");
      cache.add(Entry.createCacheEntry(pr1));

      Assert.assertNull(listener.resourceEvicted);
      Assert.assertEquals(2, cache.getSize());

      manualClock.increment(ResourceCache.EVICTION_IDLE_TIMEOUT_DEFAULT / 2 + 1);
      cache.runEviction();

      Assert.assertEquals("rm1", listener.resourceEvicted);
      Assert.assertEquals(1, cache.getSize());

      manualClock.increment(ResourceCache.EVICTION_IDLE_TIMEOUT_DEFAULT / 2 + 1);
      cache.runEviction();

      Assert.assertEquals("rm2", listener.resourceEvicted);
      Assert.assertEquals(0, cache.getSize());

    } finally {
      cache.stop();
    }
  }

  @Test
  public void testCacheRemoveById() throws Exception {
    CacheListener listener = new CacheListener();
    ResourceCache cache = new ResourceCache("q", new Configuration(false),
        listener);
    try {
      cache.start();

      Resource r1 = TestUtils.createResource("l1",
          Resource.Locality.MUST, 1, 1024);
      PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl(r1);
      pr1.setAllocationInfo("l1", 1, 1024);
      pr1.setRmResourceId("rm1");
      Entry entry = Entry.createCacheEntry(pr1);
      UUID id1 = entry.getResourceId();
      cache.add(entry);

      pr1.setAllocationInfo("l1", 1, 1024);
      pr1.setRmResourceId("rm2");
      entry = Entry.createCacheEntry(pr1);
      UUID id2 = entry.getResourceId();
      cache.add(entry);

      Assert.assertEquals(2, cache.getSize());

      CacheRMResource cr1 = cache.findAndRemove(id1);
      Assert.assertNotNull(cr1);
      Assert.assertEquals("rm1", cr1.getRmResourceId());

      Assert.assertEquals(1, cache.getSize());

      Assert.assertNull(cache.findAndRemove(id1));

      CacheRMResource cr2 = cache.findAndRemove(id2);
      Assert.assertNotNull(cr2);
      Assert.assertEquals("rm2", cr2.getRmResourceId());

      Assert.assertEquals(0, cache.getSize());

      Assert.assertNull(cache.findAndRemove(id2));

      Assert.assertNull(listener.resourceEvicted);

    } finally {
      cache.stop();
    }
  }

}

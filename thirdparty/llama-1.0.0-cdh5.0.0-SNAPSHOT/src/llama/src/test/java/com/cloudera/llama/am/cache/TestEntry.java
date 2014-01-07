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
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestEntry {
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
  public void testEntry() throws Exception {
    manualClock.set(1000l);
    Resource resource = TestUtils.createResource("l1",
        Resource.Locality.MUST, 1, 1024);
    PlacedResourceImpl placedResource = TestUtils.createPlacedResourceImpl(resource);
    placedResource.setAllocationInfo("l11", 2, 2048);
    placedResource.setRmResourceId("rm11");
    Entry entry1 = Entry.createCacheEntry(placedResource);
    Assert.assertNotNull(entry1.toString());
    Assert.assertFalse(entry1.isValid());
    entry1.setValid(true);
    Assert.assertTrue(entry1.isValid());
    Assert.assertNotNull(entry1.getResourceId());
    Assert.assertEquals(1000l, entry1.getCachedOn());
    Assert.assertEquals("l11", entry1.getLocation());
    Assert.assertEquals("rm11", entry1.getRmResourceId());
    Assert.assertEquals(2, entry1.getCpuVCores());
    Assert.assertEquals(2048, entry1.getMemoryMbs());

    placedResource.setAllocationInfo("l22", 2, 2048);
    placedResource.setRmResourceId("rm22");
    Entry entry2 = Entry.createCacheEntry(placedResource);
    Assert.assertTrue(entry1.compareTo(entry1) == 0);
    Assert.assertTrue(entry1.compareTo(entry2) < 0);
    Assert.assertTrue(entry2.compareTo(entry1) > 0);
  }

}

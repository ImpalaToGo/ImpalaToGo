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
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.junit.Test;

public class TestResourceStore {

  @Test
  public void testCacheRemoveById() throws Exception {
    ResourceStore store = new ResourceStore();
    Resource r1 = TestUtils.createResource("l1", Resource.Locality.MUST, 1, 1024);
    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl(r1);
    pr1.setAllocationInfo("l1", 1, 1024);
    pr1.setRmResourceId("rm1");
    Entry entry = Entry.createCacheEntry(pr1);
    UUID id1 = entry.getResourceId();
    store.add(entry);

    pr1.setAllocationInfo("l1", 1, 1024);
    pr1.setRmResourceId("rm2");
    entry = Entry.createCacheEntry(pr1);
    UUID id2 = entry.getResourceId();
    store.add(entry);

    Assert.assertEquals(2, store.getSize());

    CacheRMResource cr1 = store.findAndRemove(id1);
    Assert.assertNotNull(cr1);
    Assert.assertEquals("rm1", cr1.getRmResourceId());

    Assert.assertEquals(1, store.getSize());

    Assert.assertNull(store.findAndRemove(id1));

    CacheRMResource cr2 = store.findAndRemove(id2);
    Assert.assertNotNull(cr2);
    Assert.assertEquals("rm2", cr2.getRmResourceId());

    Assert.assertEquals(0, store.getSize());

    Assert.assertNull(store.findAndRemove(id2));
  }

  @Test
  public void testCacheBiggerFindMustLocation() throws Exception {
    ResourceStore store = new ResourceStore();

    Resource r1 = TestUtils.createResource("l1", Resource.Locality.MUST, 1, 512);
    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl(r1);
    pr1.setAllocationInfo("l1", 1, 512);
    pr1.setRmResourceId("rm1");
    store.add(Entry.createCacheEntry(pr1));

    r1 = TestUtils.createResource("l1",
        Resource.Locality.MUST, 2, 1024);
    pr1 = TestUtils.createPlacedResourceImpl(r1);
    pr1.setAllocationInfo("l1", 2, 1024);
    pr1.setRmResourceId("rm2");
    store.add(Entry.createCacheEntry(pr1));

    Resource r2 = TestUtils.createResource("l1",
        Resource.Locality.MUST, 1, 1024);
    PlacedResourceImpl pr2 = TestUtils.createPlacedResourceImpl(r2);

    CacheRMResource cr = store.findAndRemove(pr2);
    Assert.assertNotNull(cr);

    Assert.assertEquals("rm2", cr.getRmResourceId());
  }

  @Test
  public void testCacheBiggerFindAnyPreferredLocation() throws Exception {
    ResourceStore store = new ResourceStore();

    Resource r1 = TestUtils.createResource("l1", Resource.Locality.MUST, 1, 512);
    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl(r1);
    pr1.setAllocationInfo("l1", 1, 512);
    pr1.setRmResourceId("rm1");
    store.add(Entry.createCacheEntry(pr1));

    r1 = TestUtils.createResource("l1",
        Resource.Locality.MUST, 2, 1024);
    pr1 = TestUtils.createPlacedResourceImpl(r1);
    pr1.setAllocationInfo("l1", 2, 1024);
    pr1.setRmResourceId("rm2");
    store.add(Entry.createCacheEntry(pr1));

    Resource r2 = TestUtils.createResource("l2",
        Resource.Locality.PREFERRED, 1, 1024);
    PlacedResourceImpl pr2 = TestUtils.createPlacedResourceImpl(r2);

    CacheRMResource cr1 = store.findAndRemove(pr2);
    Assert.assertNotNull(cr1);
    Assert.assertEquals("rm2", cr1.getRmResourceId());

    store.add(Entry.createCacheEntry(pr1));

    Resource r3 = TestUtils.createResource("l2",
        Resource.Locality.DONT_CARE, 1, 1024);
    PlacedResourceImpl pr3 = TestUtils.createPlacedResourceImpl(r3);

    CacheRMResource cr2 = store.findAndRemove(pr3);
    Assert.assertNotNull(cr2);

    Assert.assertEquals("rm2", cr2.getRmResourceId());
  }

  @Test
  public void testCacheExactFindPreferredAndAnyLocation() throws Exception {
    ResourceStore store = new ResourceStore();

    Resource r1 = TestUtils.createResource("l1", Resource.Locality.MUST, 1, 1024);
    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl(r1);
    pr1.setAllocationInfo("l1", 1, 1024);
    pr1.setRmResourceId("rm1");
    store.add(Entry.createCacheEntry(pr1));
    store.add(Entry.createCacheEntry(pr1));

    Resource r2 = TestUtils.createResource("l2",
        Resource.Locality.PREFERRED, 1, 1024);
    PlacedResourceImpl pr2 = TestUtils.createPlacedResourceImpl(r2);
    CacheRMResource cr = store.findAndRemove(pr2);
    Assert.assertNotNull(cr);

    Resource r3 = TestUtils.createResource("l2",
        Resource.Locality.DONT_CARE, 1, 1024);
    PlacedResourceImpl pr3 = TestUtils.createPlacedResourceImpl(r3);
    cr = store.findAndRemove(pr3);
    Assert.assertNotNull(cr);

    Assert.assertEquals("l1", cr.getLocation());
    Assert.assertTrue(cr.getCpuVCores() >= pr2.getCpuVCores());
    Assert.assertTrue(cr.getMemoryMbs() >= pr2.getMemoryMbs());
    Assert.assertEquals(0, store.getSize());
  }

  @Test
  public void testCacheMissMustLocation() throws Exception {
    ResourceStore store = new ResourceStore();

    Resource r1 = TestUtils.createResource("l1", Resource.Locality.MUST, 1, 1024);
    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl(r1);
    pr1.setAllocationInfo("l1", 1, 1024);
    pr1.setRmResourceId("rm1");
    store.add(Entry.createCacheEntry(pr1));

    Resource r2 = TestUtils.createResource("l1",
        Resource.Locality.MUST, 2, 1024);
    PlacedResourceImpl pr2 = TestUtils.createPlacedResourceImpl(r2);

    Assert.assertNull(store.findAndRemove(pr2));

    Resource r3 = TestUtils.createResource("l1",
        Resource.Locality.MUST, 1, 2048);
    PlacedResourceImpl pr3 = TestUtils.createPlacedResourceImpl(r3);

    Assert.assertNull(store.findAndRemove(pr3));

    Resource r4 = TestUtils.createResource("l2",
        Resource.Locality.MUST, 1, 1024);
    PlacedResourceImpl pr4 = TestUtils.createPlacedResourceImpl(r4);

    Assert.assertNull(store.findAndRemove(pr4));

    Assert.assertEquals(1, store.getSize());
  }

  @Test
  public void testCacheMissPreferredAndAnyLocation() throws Exception {
    ResourceStore store = new ResourceStore();

    Resource r1 = TestUtils.createResource("l1", Resource.Locality.MUST, 1, 1024);
    PlacedResourceImpl pr1 = TestUtils.createPlacedResourceImpl(r1);
    pr1.setAllocationInfo("l1", 1, 1024);
    pr1.setRmResourceId("rm1");
    store.add(Entry.createCacheEntry(pr1));

    Resource r2 = TestUtils.createResource("l2",
        Resource.Locality.PREFERRED, 2, 1024);
    PlacedResourceImpl pr2 = TestUtils.createPlacedResourceImpl(r2);
    Assert.assertNull(store.findAndRemove(pr2));

    Resource r3 = TestUtils.createResource("l1",
        Resource.Locality.MUST, 1, 2048);
    PlacedResourceImpl pr3 = TestUtils.createPlacedResourceImpl(r3);

    Assert.assertNull(store.findAndRemove(pr3));

    Assert.assertEquals(1, store.getSize());
  }

}

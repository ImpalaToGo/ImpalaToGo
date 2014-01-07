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
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;

public class TestRMResourceChange {

  @Test
  public void testAllocate() {
    UUID cId = UUID.randomUUID();
    RMEvent c = RMEvent.createAllocationEvent(cId, "l", 1, 2, "id",
    new HashMap<String, Object>());
    c.toString();
    Assert.assertEquals(cId, c.getResourceId());
    Assert.assertEquals("id", c.getRmResourceId());
    Assert.assertEquals(1, c.getCpuVCores());
    Assert.assertEquals(2, c.getMemoryMbs());
    Assert.assertEquals("l", c.getLocation());
    Assert.assertEquals(PlacedResource.Status.ALLOCATED, c.getStatus());
  }

  @Test
  public void testNonAllocate() {
    UUID cId = UUID.randomUUID();
    RMEvent c = RMEvent.createStatusChangeEvent(cId,
        PlacedResource.Status.LOST);
    c.toString();
    Assert.assertEquals(cId, c.getResourceId());
    Assert.assertNull(c.getRmResourceId());
    Assert.assertEquals(-1, c.getCpuVCores());
    Assert.assertEquals(-1, c.getMemoryMbs());
    Assert.assertNull(c.getLocation());
    Assert.assertEquals(PlacedResource.Status.LOST, c.getStatus());
  }
}

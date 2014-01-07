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

import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.am.api.Resource;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.ManualClock;
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPlacedResourceImpl {
  private ManualClock clock;

  @Before
  public void setUp() {
    clock = new ManualClock(1);
    Clock.setClock(clock);
  }

  @After
  public void cleanUp() {
    Clock.setClock(Clock.SYSTEM);
  }

  @Test
  public void testImpl() {
    Reservation r = TestUtils.createReservation(true);
    PlacedReservationImpl i = new PlacedReservationImpl(UUID.randomUUID(), r);

    clock.increment(1);

    Resource rr = TestUtils.createResource("n");
    PlacedResourceImpl ii = PlacedResourceImpl.createPlaced(i, rr);
    Assert.assertNotNull(ii.toString());
    Assert.assertNotNull(ii.getResourceId());
    Assert.assertEquals(PlacedResource.Status.PENDING, ii.getStatus());
    Assert.assertEquals(i.getReservationId(), ii.getReservationId());
    Assert.assertEquals(i.getUser(), ii.getUser());
    Assert.assertEquals(i.getQueue(), ii.getQueue());
    Assert.assertEquals(i.getPlacedOn(), ii.getPlacedOn());
    Assert.assertEquals(rr.getLocationAsk(), ii.getLocationAsk());
    Assert.assertEquals(rr.getLocalityAsk(), ii.getLocalityAsk());
    Assert.assertEquals(rr.getCpuVCoresAsk(), ii.getCpuVCoresAsk());
    Assert.assertEquals(rr.getMemoryMbsAsk(), ii.getMemoryMbsAsk());
    Assert.assertNull(ii.getLocation());
    Assert.assertEquals(-1, ii.getCpuVCores());
    Assert.assertEquals(-1, ii.getMemoryMbs());
    ii.setStatus(PlacedResource.Status.LOST);
    Assert.assertNull(ii.getLocation());
    Assert.assertEquals(-1, ii.getCpuVCores());
    Assert.assertEquals(-1, ii.getMemoryMbs());
    ii.setAllocationInfo("nn", 2, 4);
    Assert.assertEquals("nn", ii.getLocation());
    Assert.assertEquals(2, ii.getCpuVCores());
    Assert.assertEquals(4, ii.getMemoryMbs());
    Assert.assertNotNull(ii.getRmData());
  }
}

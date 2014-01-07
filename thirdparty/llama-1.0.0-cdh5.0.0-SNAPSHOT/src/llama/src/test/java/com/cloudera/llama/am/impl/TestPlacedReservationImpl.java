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

import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.ManualClock;
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPlacedReservationImpl {
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
    UUID id = UUID.randomUUID();
    PlacedReservationImpl i = new PlacedReservationImpl(id, r);
    Assert.assertNotNull(i.toString());
    Assert.assertEquals(id, i.getReservationId());
    Assert.assertEquals("u", i.getUser());
    Assert.assertEquals("q", i.getQueue());
    Assert.assertEquals(clock.currentTimeMillis(), i.getPlacedOn());
    Assert.assertEquals(PlacedReservation.Status.PENDING, i.getStatus());
    Assert.assertNull(i.getExpansionOf());
    Assert.assertEquals(-1, i.getAllocatedOn());
    Assert.assertTrue(i.isGang());
    Assert.assertFalse(i.isQueued());
    Assert.assertEquals(1, i.getResources().size());
    Assert.assertEquals(1, i.getPlacedResources().size());
    TestUtils.assertResource(r.getResources().get(0), i.getResources().get(0));
    TestUtils.assertResource(r.getResources().get(0),
        i.getPlacedResources().get(0));
    TestUtils.assertResource(r.getResources().get(0),
        i.getPlacedResourceImpls().get(0));
    clock.increment(1);
    i.setStatus(PlacedReservation.Status.LOST);
    Assert.assertEquals(PlacedReservation.Status.LOST, i.getStatus());
    Assert.assertEquals(-1, i.getAllocatedOn());
    i.setStatus(PlacedReservation.Status.ALLOCATED);
    Assert.assertEquals(clock.currentTimeMillis(), i.getAllocatedOn());
    i.setQueued(true);
    Assert.assertTrue(i.isQueued());
  }
}

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
import com.cloudera.llama.am.api.LlamaAMEvent;
import com.cloudera.llama.am.api.LlamaAMListener;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.Resource;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.am.impl.ThrottleLlamaAM;
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestLlamaAMWithMock {

  public static class MockListener implements LlamaAMListener {
    public List<LlamaAMEvent> events = Collections.synchronizedList(
        new ArrayList<LlamaAMEvent>());

    @Override
    public void onEvent(LlamaAMEvent event) {
      events.add(event);
    }
  }

  protected Configuration getConfiguration() {
    Configuration conf = new Configuration(false);
    conf.set("llama.am.mock.queues", "q1,q2");
    conf.set("llama.am.mock.events.min.wait.ms", "10");
    conf.set("llama.am.mock.events.max.wait.ms", "10");
    conf.set("llama.am.mock.nodes", "h0,h1,h2,h3");
    conf.set(LlamaAM.CORE_QUEUES_KEY, "q1");
    conf.set(LlamaAM.RM_CONNECTOR_CLASS_KEY, MockRMConnector.class.getName());
    conf.setInt(ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_KEY, 1000000);
    conf.setInt(ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_KEY, 1000000);
    return conf;
  }

  @Test
  public void testMocks() throws Exception {
    final LlamaAM llama = LlamaAM.create(getConfiguration());
    MockListener listener = new MockListener();
    try {
      llama.start();
      llama.addListener(listener);
      Resource a1 = TestUtils.createResource(MockLlamaAMFlags.ALLOCATE + "h0",
          Resource.Locality.DONT_CARE, 1, 1);
      Resource a2 = TestUtils.createResource(MockLlamaAMFlags.REJECT + "h1",
          Resource.Locality.DONT_CARE, 1, 1);
      Resource a3 = TestUtils.createResource(MockLlamaAMFlags.PREEMPT + "h2",
          Resource.Locality.DONT_CARE, 1, 1);
      Resource a4 = TestUtils.createResource(MockLlamaAMFlags.LOSE + "h3",
          Resource.Locality.DONT_CARE, 1, 1);
      PlacedReservation pr1 = llama.getReservation(
          llama.reserve(TestUtils.createReservation(
          UUID.randomUUID(), "u", "q1", a1, true)));
      PlacedReservation pr2 = llama.getReservation(
          llama.reserve(TestUtils.createReservation(
          UUID.randomUUID(), "u", "q1", a2, true)));
      PlacedReservation pr3 = llama.getReservation(
          llama.reserve(TestUtils.createReservation(
          UUID.randomUUID(), "u", "q1", a3, true)));
      PlacedReservation pr4 = llama.getReservation(
          llama.reserve(TestUtils.createReservation(
          UUID.randomUUID(), "u", "q1", a4, true)));
      Thread.sleep(100);
      //for gang reservations, ALLOCATED to PREEMPTED/LOST don't finish reservation
      Assert.assertEquals(8,
          TestUtils.getReservations(listener.events, null, true).size());
      Set<UUID> allocated = new HashSet<UUID>();
      allocated.add(pr1.getPlacedResources().get(0).getResourceId());
      allocated.add(pr3.getPlacedResources().get(0).getResourceId());
      allocated.add(pr4.getPlacedResources().get(0).getResourceId());
      Set<UUID> rejected = new HashSet<UUID>();
      rejected.add(pr2.getPlacedResources().get(0).getResourceId());
      Set<UUID> lost = new HashSet<UUID>();
      lost.add(pr4.getPlacedResources().get(0).getResourceId());
      Set<UUID> preempted = new HashSet<UUID>();
      preempted.add(pr3.getPlacedResources().get(0).getResourceId());
      for (LlamaAMEvent event : listener.events) {
        for (PlacedResource r : event.getResourceChanges()) {
          if (r.getStatus() == PlacedResource.Status.ALLOCATED) {
            allocated.remove(r.getResourceId());
          }
          if (r.getStatus() == PlacedResource.Status.REJECTED) {
            rejected.remove(r.getResourceId());
          }
          if (r.getStatus() == PlacedResource.Status.LOST) {
            lost.remove(r.getResourceId());
          }
          if (r.getStatus() == PlacedResource.Status.PREEMPTED) {
            preempted.remove(r.getResourceId());
          }
        }
      }
      Set<UUID> remaining = new HashSet<UUID>();
      remaining.addAll(allocated);
      remaining.addAll(rejected);
      remaining.addAll(lost);
      remaining.addAll(preempted);
      Assert.assertTrue(remaining.isEmpty());

      listener.events.clear();
      UUID c5 = UUID.randomUUID();
      Resource a5 = TestUtils.createResource(MockLlamaAMFlags.ALLOCATE + "XX",
          Resource.Locality.DONT_CARE, 1, 1);
      llama.reserve(TestUtils.createReservation(UUID.randomUUID(), "u", "q1", a5,
          true));
      Thread.sleep(100);
      Assert.assertEquals(2, TestUtils.getReservations(listener.events, null, true).size());
      Assert.assertEquals(1, TestUtils.getReservations(listener.events, PlacedReservation.Status.REJECTED, false).size());
      Assert.assertEquals(1, TestUtils.getResources(listener.events, PlacedResource.Status.REJECTED, false).size());
    } finally {
      llama.stop();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidQueue() throws Exception {
    final LlamaAM llama = LlamaAM.create(getConfiguration());
    try {
      llama.start();
      UUID c1 = UUID.randomUUID();
      Resource a1 = TestUtils.createResource(MockLlamaAMFlags.ALLOCATE + "h0",
          Resource.Locality.DONT_CARE, 1, 1);
      llama.reserve(TestUtils.createReservation(UUID.randomUUID(), "u",
          "invalid-q", a1, false));
    } finally {
      llama.stop();
    }
  }

  private boolean hasAllStatus(List<LlamaAMEvent> events) {
    events = new ArrayList<LlamaAMEvent>(events);
    boolean lost = false;
    boolean rejected = false;
    boolean preempted = false;
    boolean allocated = false;
    for (LlamaAMEvent event : events) {
      for (PlacedResource r : event.getResourceChanges()) {
        if (r.getStatus() == PlacedResource.Status.ALLOCATED) {
          allocated = true;
        }
        if (r.getStatus() == PlacedResource.Status.REJECTED) {
          rejected = true;
        }
        if (r.getStatus() == PlacedResource.Status.LOST) {
          lost = true;
        }
        if (r.getStatus() == PlacedResource.Status.PREEMPTED) {
          preempted = true;
        }
      }
    }
    return lost && rejected && preempted && allocated;
  }

  @Test
  public void testRandom() throws Exception {
    final LlamaAM llama = LlamaAM.create(getConfiguration());
    MockListener listener = new MockListener();
    try {
      llama.start();
      llama.addListener(listener);
      while (!hasAllStatus(listener.events)) {
        Resource a1 = TestUtils.createResource("h0",
            Resource.Locality.DONT_CARE, 1, 1);
        llama.reserve(TestUtils.createReservation(UUID.randomUUID(), "u", "q1",
            a1, false));
      }
    } finally {
      llama.stop();
    }
  }

}

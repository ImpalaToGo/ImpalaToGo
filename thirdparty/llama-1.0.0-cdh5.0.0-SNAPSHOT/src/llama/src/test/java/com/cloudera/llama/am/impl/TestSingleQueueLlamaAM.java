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
import com.cloudera.llama.am.api.LlamaAMEvent;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.am.api.LlamaAMListener;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.am.api.Resource;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.am.spi.RMEvent;
import com.cloudera.llama.am.spi.RMListener;
import com.cloudera.llama.am.spi.RMConnector;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

public class TestSingleQueueLlamaAM {

  public static class MyRMConnector implements RMConnector {

    public boolean start = false;
    public boolean stop = false;
    public boolean reserve = false;
    public boolean release = false;
    public boolean doNotCache = false;
    public RMListener callback;

    protected MyRMConnector() {
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
      start = true;
    }

    @Override
    public void unregister() {
      stop = true;
    }

    @Override
    public List<String> getNodes() throws LlamaException {
      return Arrays.asList("node");
    }

    @Override
    public void reserve(Collection<RMResource> resources)
        throws LlamaException {
      reserve = true;
    }

    @Override
    public void release(Collection<RMResource> resources, boolean doNotCache)
        throws LlamaException {
      release = true;
      this.doNotCache = doNotCache;
    }

    public void emptyCache() throws LlamaException {
    }

    @Override
    public boolean reassignResource(Object rmResourceId, UUID resourceId) {
      return false;
    }

    @Override
    public void setMetricRegistry(MetricRegistry registry) {
    }
  }

  public static class DummyLlamaAMListener implements LlamaAMListener {
    public List<LlamaAMEvent> events = new ArrayList<LlamaAMEvent>();

    @Override
    public void onEvent(LlamaAMEvent event) {
      events.add(event);
    }
  }

  public static class DummySingleQueueLlamaAMCallback implements
      IntraLlamaAMsCallback {

    @Override
    public void discardReservation(UUID reservationId) {
    }

    @Override
    public void discardAM(String queue) {
    }
  }

  private SingleQueueLlamaAM createLlamaAM() {
    Configuration conf = new Configuration(false);
    conf.setClass(LlamaAM.RM_CONNECTOR_CLASS_KEY, MyRMConnector.class,
        RMConnector.class);
    conf.setBoolean(LlamaAM.CACHING_ENABLED_KEY, false);
    SingleQueueLlamaAM am = new SingleQueueLlamaAM(conf, "queue");
    am.setCallback(new DummySingleQueueLlamaAMCallback());
    return am;
  }

  @Test
  public void testRmStartStop() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    try {
      Assert.assertFalse(llama.isRunning());
      llama.start();
      Assert.assertTrue(((MyRMConnector) llama.getRMConnector()).start);
      Assert.assertTrue(llama.isRunning());
      Assert.assertFalse(((MyRMConnector) llama.getRMConnector()).stop);
    } finally {
      llama.stop();
      Assert.assertFalse(llama.isRunning());
      Assert.assertTrue(((MyRMConnector) llama.getRMConnector()).stop);
      llama.stop();
    }
  }

  @Test
  public void testRmStopNoRMConnector() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    llama.stop();
  }

  private static final Resource RESOURCE1 = TestUtils.createResource(
      "n1", Resource.Locality.DONT_CARE, 1, 1024);

  private static final Resource RESOURCE2 = TestUtils.createResource(
      "n2", Resource.Locality.PREFERRED, 2, 2048);

  private static final Resource RESOURCE3 = TestUtils.createResource(
      "n3", Resource.Locality.PREFERRED, 3, 2048);

  private static final List<Resource> RESOURCES1 = Arrays.asList(RESOURCE1);

  private static final List<Resource> RESOURCES2 = Arrays.asList(RESOURCE1,
      RESOURCE2);

  private static final Reservation RESERVATION1_GANG = 
      TestUtils.createReservation(UUID.randomUUID(), "u", "queue", RESOURCES1, 
          true);

  private static final Reservation RESERVATION2_GANG = 
      TestUtils.createReservation(UUID.randomUUID(), "u","queue", RESOURCES2, 
          true);

  private static final Reservation RESERVATION1_NONGANG = 
      TestUtils.createReservation(UUID.randomUUID(), "u","queue", RESOURCES1, 
          false);

  private static final Reservation RESERVATION2_NONGANG = 
      TestUtils.createReservation(UUID.randomUUID(), "u","queue", RESOURCES2, 
          false);

  @Test
  public void testGetNode() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    try {
      llama.start();
      llama.reserve(RESERVATION1_NONGANG);
      Assert.assertEquals(Arrays.asList("node"), llama.getNodes());
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testRmReserve() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    try {
      llama.start();
      UUID reservationId = llama.reserve(RESERVATION1_NONGANG);

      Assert.assertTrue(((MyRMConnector) llama.getRMConnector()).reserve);
      Assert.assertFalse(((MyRMConnector) llama.getRMConnector()).release);

      PlacedReservation placedReservation = llama.getReservation(reservationId);
      Assert.assertNotNull(placedReservation);
      Assert.assertEquals(PlacedReservation.Status.PENDING,
          placedReservation.getStatus());
      Assert.assertEquals(reservationId, placedReservation.getReservationId());
      Assert.assertEquals("queue", placedReservation.getQueue());
      Assert.assertFalse(placedReservation.isGang());
      Assert.assertEquals(1, placedReservation.getResources().size());
      PlacedResource resource = placedReservation.getPlacedResources().get(0);
      Assert.assertEquals(PlacedResource.Status.PENDING, resource.getStatus());
      Assert.assertEquals(-1, resource.getCpuVCores());
      Assert.assertEquals(-1, resource.getMemoryMbs());
      Assert.assertEquals(null, resource.getLocation());
      Assert.assertEquals("queue", resource.getQueue());
      Assert.assertEquals(reservationId, resource.getReservationId());
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testRmRelease() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    try {
      llama.start();
      UUID reservationId = llama.reserve(RESERVATION1_NONGANG);
      Assert.assertNotNull(llama.releaseReservation(
          RESERVATION1_NONGANG.getHandle(), reservationId, true));
      Assert.assertTrue(((MyRMConnector) llama.getRMConnector()).release);
      Assert.assertTrue(((MyRMConnector) llama.getRMConnector()).doNotCache);
      Assert.assertNull(llama._getReservation(reservationId));
    } finally {
      llama.stop();
    }
  }

  @Test(expected = LlamaException.class)
  public void testReleaseUsingWrongHandle() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    try {
      llama.start();
      UUID reservationId = llama.reserve(RESERVATION1_NONGANG);
      llama.releaseReservation(UUID.randomUUID(), reservationId, false);
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testAdminRelease() throws Exception {
    final SingleQueueLlamaAM llama = createLlamaAM();
    try {
      llama.start();
      final UUID reservationId = llama.reserve(RESERVATION1_NONGANG);
      LlamaAM.doAsAdmin(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Assert.assertNotNull(llama.releaseReservation(
              LlamaAM.WILDCARD_HANDLE, reservationId, false));
          return null;
        }
      });
      Assert.assertTrue(((MyRMConnector) llama.getRMConnector()).release);
      Assert.assertNull(llama._getReservation(reservationId));
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testFullyAllocateReservationNoGangOneResource() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION1_NONGANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      RMEvent change = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, "cid1", new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change));

      List<PlacedResource> resources = TestUtils.getResources(listener.events,
          PlacedResource.Status.ALLOCATED, false);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events, PlacedReservation.Status.ALLOCATED, false);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(1, reservations.size());
      PlacedResource resource = resources.get(0);
      Assert.assertEquals(pr.getPlacedResources().get(0).getRmResourceId(),
          resource.getRmResourceId());
      Assert.assertEquals(3, resource.getCpuVCores());
      Assert.assertEquals(4096, resource.getMemoryMbs());
      Assert.assertEquals("a1", resource.getLocation());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.ALLOCATED,
          reservation.getStatus());
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testFullyAllocateReservationGangOneResource() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION1_GANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      RMEvent change = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(), 
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change));
      List<PlacedResource> resources = TestUtils.getResources(listener.events,
          PlacedResource.Status.ALLOCATED, false);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events, PlacedReservation.Status.ALLOCATED, false);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(1, reservations.size());
      PlacedResource resource = resources.get(0);
      Assert.assertEquals(pr.getPlacedResources().get(0).getRmResourceId(),
          resource.getRmResourceId());
      Assert.assertEquals(3, resource.getCpuVCores());
      Assert.assertEquals(4096, resource.getMemoryMbs());
      Assert.assertEquals("a1", resource.getLocation());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.ALLOCATED,
          reservation.getStatus());
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testFullyAllocateReservationNoGangTwoResources()
      throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION2_NONGANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      UUID resource2Id = pr.getPlacedResources().get(1).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(), 
              new HashMap<String, Object>());
      RMEvent change2 = RMEvent.createAllocationEvent
          (resource2Id, "a2", 4, 5112, new Object(), 
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1, change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events,
          PlacedResource.Status.ALLOCATED, false);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events, PlacedReservation.Status.ALLOCATED, false);
      Assert.assertEquals(2, resources.size());
      Assert.assertEquals(1, reservations.size());
      PlacedResource resource1 = resources.get(0);
      PlacedResource resource2 = resources.get(1);
      Assert.assertEquals(pr.getPlacedResources().get(0).getRmResourceId(),
          resource1.getRmResourceId());
      Assert.assertEquals(pr.getPlacedResources().get(1).getRmResourceId(),
          resource2.getRmResourceId());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.ALLOCATED,
          reservation.getStatus());
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testFullyAllocateReservationGangTwoResources() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION2_GANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      UUID resource2Id = pr.getPlacedResources().get(1).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      RMEvent change2 = RMEvent.createAllocationEvent
          (resource2Id, "a2", 4, 5112, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1, change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events,
          PlacedResource.Status.ALLOCATED, false);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events, PlacedReservation.Status.ALLOCATED, false);
      Assert.assertEquals(2, resources.size());
      Assert.assertEquals(1, reservations.size());
      PlacedResource resource1 = resources.get(0);
      PlacedResource resource2 = resources.get(1);
      Assert.assertEquals(pr.getPlacedResources().get(0).getRmResourceId(),
          resource1.getRmResourceId());
      Assert.assertEquals(pr.getPlacedResources().get(1).getRmResourceId(),
          resource2.getRmResourceId());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.ALLOCATED,
          reservation.getStatus());

    } finally {
      llama.stop();
    }
  }

  @Test
  public void testPartiallyThenFullyAllocateReservationNoGangTwoResources()
      throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION2_NONGANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      UUID resource2Id = pr.getPlacedResources().get(1).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      List<PlacedResource> resources = TestUtils.getResources(listener.events,
          PlacedResource.Status.ALLOCATED, false);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events, PlacedReservation.Status.ALLOCATED, false);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(0, reservations.size());
      PlacedResource resource1 = resources.get(0);
      Assert.assertEquals(pr.getPlacedResources().get(0).getRmResourceId(),
          resource1.getRmResourceId());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.PARTIAL,
          reservation.getStatus());
      RMEvent change2 = RMEvent.createAllocationEvent
          (resource2Id, "a2", 4, 5112, new Object(),
              new HashMap<String, Object>());
      listener.events.clear();
      llama.onEvent(Arrays.asList(change2));
      resources = TestUtils.getResources(listener.events,
          PlacedResource.Status.ALLOCATED, false);
      reservations = TestUtils.getReservations(listener.events,
          PlacedReservation.Status.ALLOCATED, false);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(1, reservations.size());
      PlacedResource resource2 = resources.get(0);
      Assert.assertEquals(pr.getPlacedResources().get(1).getRmResourceId(),
          resource2.getRmResourceId());
      reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.ALLOCATED,
          reservation.getStatus());

    } finally {
      llama.stop();
    }
  }

  @Test
  public void testPartiallyThenFullyAllocateReservationGangTwoResources()
      throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION2_GANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      UUID resource2Id = pr.getPlacedResources().get(1).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      Assert.assertEquals(1, TestUtils.getResources(listener.events, PlacedResource.Status.ALLOCATED, false).size());
      Assert.assertEquals(1, TestUtils.getReservations(listener.events, PlacedReservation.Status.PARTIAL, false).size());
      listener.events.clear();
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.PARTIAL,
          reservation.getStatus());
      RMEvent change2 = RMEvent.createAllocationEvent
          (resource2Id, "a2", 4, 5112, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change2));
      Assert.assertEquals(1, TestUtils.getResources(listener.events, PlacedResource.Status.ALLOCATED, false).size());
      Assert.assertEquals(1, TestUtils.getReservations(listener.events, PlacedReservation.Status.ALLOCATED, false).size());
      List<PlacedResource> resources = TestUtils.getReservations(listener.events,
          PlacedReservation.Status.ALLOCATED, false).get(0).getPlacedResources();
      PlacedResource resource1 = resources.get(0);
      PlacedResource resource2 = resources.get(1);
      Assert.assertEquals(resource1.getRmResourceId(),
          resource1.getRmResourceId());
      Assert.assertEquals(resource2.getRmResourceId(),
          resource2.getRmResourceId());
      reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.ALLOCATED,
          reservation.getStatus());

    } finally {
      llama.stop();
    }
  }

  @Test
  public void testRejectPendingReservation() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION1_GANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      RMEvent change = RMEvent.createStatusChangeEvent
          (resource1Id, PlacedResource.Status.REJECTED);
      llama.onEvent(Arrays.asList(change));
      List<PlacedResource> resources = TestUtils.getResources(listener.events,
          PlacedResource.Status.REJECTED, false);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events, PlacedReservation.Status.REJECTED, false);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(1, reservations.size());
      Assert.assertNull(llama.getReservation(reservationId));
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testRejectPartialGangReservation() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION2_GANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      UUID resource2Id = pr.getPlacedResources().get(1).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      RMEvent change2 = RMEvent.createStatusChangeEvent
          (resource2Id, PlacedResource.Status.REJECTED);
      listener.events.clear();
      llama.onEvent(Arrays.asList(change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events.get(0),
          PlacedResource.Status.REJECTED);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.REJECTED);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(1, reservations.size());
      Assert.assertNull(llama.getReservation(reservationId));
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testRejectPartialNonGangReservation() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION2_NONGANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      UUID resource2Id = pr.getPlacedResources().get(1).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      RMEvent change2 = RMEvent.createStatusChangeEvent
          (resource2Id, PlacedResource.Status.REJECTED);
      listener.events.clear();
      llama.onEvent(Arrays.asList(change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events.get(0),
          PlacedResource.Status.REJECTED);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.REJECTED);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(0, reservations.size());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.PARTIAL,
          reservation.getStatus());
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testPreemptPartialGangReservation() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION2_GANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      RMEvent change2 = RMEvent.createStatusChangeEvent
          (resource1Id, PlacedResource.Status.PREEMPTED);
      listener.events.clear();
      llama.onEvent(Arrays.asList(change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events.get(0),
          PlacedResource.Status.PREEMPTED);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.PREEMPTED);
      Assert.assertEquals(0, resources.size());
      Assert.assertEquals(1, reservations.size());
      Assert.assertNull(llama.getReservation(reservationId));
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testPreemptPartialNonGangReservation() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION2_NONGANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      RMEvent change2 = RMEvent.createStatusChangeEvent
          (resource1Id, PlacedResource.Status.PREEMPTED);
      listener.events.clear();
      llama.onEvent(Arrays.asList(change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events.get(0),
          PlacedResource.Status.PREEMPTED);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.PREEMPTED);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(0, reservations.size());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.PARTIAL,
          reservation.getStatus());
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testPreemptAllocatedGangReservation() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION1_GANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      RMEvent change2 = RMEvent.createStatusChangeEvent
          (resource1Id, PlacedResource.Status.PREEMPTED);
      listener.events.clear();
      llama.onEvent(Arrays.asList(change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events.get(0),
          PlacedResource.Status.PREEMPTED);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.PREEMPTED);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(0, reservations.size());
      reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.REJECTED);
      Assert.assertEquals(0, reservations.size());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.ALLOCATED,
          reservation.getStatus());
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testPreemptAllocatedNonGangReservation() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION1_NONGANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      RMEvent change2 = RMEvent.createStatusChangeEvent
          (resource1Id, PlacedResource.Status.PREEMPTED);
      listener.events.clear();
      llama.onEvent(Arrays.asList(change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events.get(0),
          PlacedResource.Status.PREEMPTED);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.PREEMPTED);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(0, reservations.size());
      reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.REJECTED);
      Assert.assertEquals(0, reservations.size());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.ALLOCATED,
          reservation.getStatus());
    } finally {
      llama.stop();
    }
  }


  @Test
  public void testLostPartialGangReservation() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION2_GANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      RMEvent change2 = RMEvent.createStatusChangeEvent
          (resource1Id, PlacedResource.Status.LOST);
      listener.events.clear();
      llama.onEvent(Arrays.asList(change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events.get(0),
          PlacedResource.Status.PREEMPTED);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.LOST);
      Assert.assertEquals(0, resources.size());
      Assert.assertEquals(1, reservations.size());
      Assert.assertNull(llama.getReservation(reservationId));
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testLostPartialNonGangReservation() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION2_NONGANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      RMEvent change2 = RMEvent.createStatusChangeEvent
          (resource1Id, PlacedResource.Status.LOST);
      listener.events.clear();
      llama.onEvent(Arrays.asList(change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events.get(0),
          PlacedResource.Status.LOST);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events.get(0), null);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(0, reservations.size());
      reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.REJECTED);
      Assert.assertEquals(0, reservations.size());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.PARTIAL,
          reservation.getStatus());
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testLostAllocatedGangReservation() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION1_GANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      RMEvent change2 = RMEvent.createStatusChangeEvent
          (resource1Id, PlacedResource.Status.LOST);
      listener.events.clear();
      llama.onEvent(Arrays.asList(change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events.get(0),
          PlacedResource.Status.LOST);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.PREEMPTED);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(0, reservations.size());
      reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.REJECTED);
      Assert.assertEquals(0, reservations.size());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.ALLOCATED,
          reservation.getStatus());
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testLostAllocatedNonGangReservation() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      llama.start();
      llama.addListener(listener);
      UUID reservationId = llama.reserve(RESERVATION1_NONGANG);
      PlacedReservation pr = llama.getReservation(reservationId);
      UUID resource1Id = pr.getPlacedResources().get(0).getResourceId();
      RMEvent change1 = RMEvent.createAllocationEvent
          (resource1Id, "a1", 3, 4096, new Object(),
              new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      RMEvent change2 = RMEvent.createStatusChangeEvent
          (resource1Id, PlacedResource.Status.LOST);
      listener.events.clear();
      llama.onEvent(Arrays.asList(change2));
      List<PlacedResource> resources = TestUtils.getResources(listener.events.get(0),
          PlacedResource.Status.LOST);
      List<PlacedReservation> reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.PREEMPTED);
      Assert.assertEquals(1, resources.size());
      Assert.assertEquals(0, reservations.size());
      reservations = TestUtils.getReservations(
          listener.events.get(0), PlacedReservation.Status.REJECTED);
      Assert.assertEquals(0, reservations.size());
      PlacedReservation reservation = llama.getReservation(reservationId);
      Assert.assertNotNull(reservation);
      Assert.assertEquals(PlacedReservation.Status.ALLOCATED,
          reservation.getStatus());
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testUnknownResourceRmChange() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      RMEvent change1 = RMEvent.createAllocationEvent(
          UUID.randomUUID(), "a1", 3, 4096, new Object(),
          new HashMap<String, Object>());
      llama.onEvent(Arrays.asList(change1));
      Assert.assertTrue(listener.events.isEmpty());
    } finally {
      llama.stop();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRmChangesNull() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    try {
      llama.start();
      llama.onEvent(null);
    } finally {
      llama.stop();
    }
  }

  @Test
  public void testReleaseReservationsForHandle() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    try {
      llama.start();
      UUID cId1 = UUID.randomUUID();
      UUID cId2 = UUID.randomUUID();
      UUID reservationId1 = llama.reserve(TestUtils.createReservation(cId1, "u",
          "queue", Arrays.asList(RESOURCE1), true));
      UUID reservationId2 = llama.reserve(TestUtils.createReservation(cId1, "u",
          "queue", Arrays.asList(RESOURCE2), true));
      UUID reservationId3 = llama.reserve(TestUtils.createReservation(cId2, "u",
          "queue", Arrays.asList(RESOURCE3), true));
      Assert.assertNotNull(llama._getReservation(reservationId1));
      Assert.assertNotNull(llama._getReservation(reservationId2));
      Assert.assertNotNull(llama._getReservation(reservationId3));
      llama.releaseReservationsForHandle(cId1, false);
      Assert.assertNull(llama._getReservation(reservationId1));
      Assert.assertNull(llama._getReservation(reservationId2));
      Assert.assertNotNull(llama._getReservation(reservationId3));
    } finally {
      llama.stop();
    }
  }


  @Test
  public void testLoseAllReservations() throws Exception {
    SingleQueueLlamaAM llama = createLlamaAM();
    DummyLlamaAMListener listener = new DummyLlamaAMListener();
    try {
      llama.start();
      llama.addListener(listener);
      UUID cId1 = UUID.randomUUID();
      UUID cId2 = UUID.randomUUID();
      UUID reservationId1 = llama.reserve(TestUtils.createReservation(cId1, "u",
          "queue", Arrays.asList(RESOURCE1), true));
      UUID reservationId2 = llama.reserve(TestUtils.createReservation(cId1, "u",
          "queue", Arrays.asList(RESOURCE2), true));
      UUID reservationId3 = llama.reserve(TestUtils.createReservation(cId2, "u",
          "queue", Arrays.asList(RESOURCE3), true));
      llama.loseAllReservations();
      Assert.assertNull(llama._getReservation(reservationId1));
      Assert.assertNull(llama._getReservation(reservationId2));
      Assert.assertNull(llama._getReservation(reservationId3));
      Assert.assertEquals(3, TestUtils.getReservations(listener.events,
          PlacedReservation.Status.LOST, false).size());
    } finally {
      llama.stop();
    }
  }
}

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
package com.cloudera.llama.am;

import com.cloudera.llama.am.api.LlamaAMEvent;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.am.api.Resource;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.am.impl.LlamaAMEventImpl;
import com.cloudera.llama.am.impl.PlacedReservationImpl;
import com.cloudera.llama.am.impl.PlacedResourceImpl;
import com.cloudera.llama.server.ClientInfo;
import com.cloudera.llama.util.UUID;
import com.cloudera.llama.util.VersionInfo;
import junit.framework.Assert;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestRestData {

  Map parseJson(String s) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(s, Map.class);
  }
  
  private PlacedReservationImpl createReservation(UUID id, UUID handle,
      String queue, PlacedReservation.Status status) {
    List<Resource> rs = new ArrayList<Resource>();
    rs.add(TestUtils.createResource("h1",
        Resource.Locality.PREFERRED, 1, 1024));
    rs.add(TestUtils.createResource("h2",
        Resource.Locality.PREFERRED, 1, 1024));
    Reservation r = TestUtils.createReservation(handle, "u", queue, rs, true);
    PlacedReservationImpl pr = new PlacedReservationImpl(id, r);
    pr.setStatus(status);
    return pr;
  }

  private void assertReservationStatus(RestData restData, UUID id,
      PlacedReservation.Status status, boolean exists) throws Exception {
    try {
      StringWriter writer = new StringWriter();
      restData.writeReservationAsJson(id, writer);
      writer.close();
      Assert.assertTrue(exists);
      Map map = parseJson(writer.toString());
      Assert.assertEquals(2, map.size());
      Assert.assertTrue(map.containsKey(RestData.REST_VERSION_KEY));
      Assert.assertEquals(RestData.REST_VERSION_VALUE, 
          map.get(RestData.REST_VERSION_KEY));
      map = (Map) map.get(RestData.RESERVATION_DATA);
      Assert.assertEquals(status.toString(), map.get("status"));
    } catch (RestData.NotFoundException ex) {
      Assert.assertFalse(exists);
    }
  }

  private void assertReservationBackedOffFlag(RestData restData, UUID id,
      boolean backedOff) throws Exception {
    StringWriter writer = new StringWriter();
    restData.writeReservationAsJson(id, writer);
    writer.close();
    Map map = parseJson(writer.toString());
    Assert.assertEquals(2, map.size());
    Assert.assertTrue(map.containsKey(RestData.REST_VERSION_KEY));
    Assert.assertEquals(RestData.REST_VERSION_VALUE, 
        map.get(RestData.REST_VERSION_KEY));
    map = (Map) map.get(RestData.RESERVATION_DATA);
    Assert.assertEquals(backedOff, map.get("hasBeenBackedOff"));
  }

  private void assertResourceStatus(RestData restData, UUID id,
      int resourceIdx, PlacedResource.Status status)
      throws Exception {
    StringWriter writer = new StringWriter();
    restData.writeReservationAsJson(id, writer);
    writer.close();
    Map map = parseJson(writer.toString());
    Assert.assertEquals(2, map.size());
    Assert.assertTrue(map.containsKey(RestData.REST_VERSION_KEY));
    Assert.assertEquals(RestData.REST_VERSION_VALUE, 
        map.get(RestData.REST_VERSION_KEY));
    map = (Map) map.get(RestData.RESERVATION_DATA);
    map = (Map) ((List)map.get("resources")).get(resourceIdx);
    Assert.assertEquals(status.toString(), map.get("status"));
  }

  private void assertReservationQueue(RestData restData, UUID id,
      String queue, boolean exists) throws Exception {
    try {
      StringWriter writer = new StringWriter();
      restData.writeQueueReservationsAsJson(queue, writer);
      writer.close();
      Assert.assertTrue(exists);
      Map map = parseJson(writer.toString());
      List reservations = (List)map.get(RestData.QUEUE_DATA);
      Assert.assertEquals(2, map.size());
      Assert.assertTrue(map.containsKey(RestData.REST_VERSION_KEY));
      Assert.assertEquals(RestData.REST_VERSION_VALUE, 
          map.get(RestData.REST_VERSION_KEY));
      Assert.assertEquals(exists, !reservations.isEmpty());
      if (exists) {
        Assert.assertEquals(id.toString(), 
            ((Map) reservations.get(0)).get("reservationId"));
      }
    } catch (RestData.NotFoundException ex) {
      Assert.assertFalse(exists);
    }
  }

  private void assertHandle(RestData restData, UUID handle, boolean exists) 
      throws Exception {
    try {
      StringWriter writer = new StringWriter();
      restData.writeHandleReservationsAsJson(handle, writer);
      writer.close();
      Assert.assertTrue(exists);
    } catch (RestData.NotFoundException ex) {
      Assert.assertFalse(exists);
    }
  }

  private void assertReservationHandle(RestData restData, UUID id,
      UUID handle, boolean exists) throws Exception {
    StringWriter writer = new StringWriter();
    restData.writeHandleReservationsAsJson(handle, writer);
    writer.close();
    Map map = parseJson(writer.toString());
    Assert.assertEquals(2, map.size());
    Assert.assertTrue(map.containsKey(RestData.REST_VERSION_KEY));
    Assert.assertEquals(RestData.REST_VERSION_VALUE, 
        map.get(RestData.REST_VERSION_KEY));
    List reservations = (List) ((Map) 
        map.get(RestData.HANDLE_DATA)).get(RestData.RESERVATIONS);
    Assert.assertEquals(exists, !reservations.isEmpty());
    if (exists) {
      Assert.assertEquals(id.toString(), 
          ((Map)reservations.get(0)).get("reservationId"));
    }
  }

  private void assertReservationNode(RestData restData, UUID id,
      String node, boolean exists) throws Exception {
    try {
      StringWriter writer = new StringWriter();
      restData.writeNodeResourcesAsJson(node, writer);
      writer.close();
      Assert.assertTrue(exists);
      Map map = parseJson(writer.toString());
      Assert.assertEquals(2, map.size());
      Assert.assertTrue(map.containsKey(RestData.REST_VERSION_KEY));
      Assert.assertEquals(RestData.REST_VERSION_VALUE, 
          map.get(RestData.REST_VERSION_KEY));
      List reservations = (List)map.get(RestData.NODE_DATA);
      Assert.assertEquals(exists, !reservations.isEmpty());
      if (exists) {
        Assert.assertEquals(id.toString(), 
            ((Map) reservations.get(0)).get("reservationId"));
      }
    } catch (RestData.NotFoundException ex) {
      Assert.assertFalse(exists);
    }
  }
  
  private LlamaAMEvent createEvents(PlacedReservation pr) {
    LlamaAMEventImpl event = new LlamaAMEventImpl();
    event.addReservation(pr);
    return event;
  }

  @Test
  public void testLifeCycleAndSelectors() throws Exception {
    RestData restData = new RestData();
    UUID id1 = UUID.randomUUID();
    final UUID handle1 = UUID.randomUUID();
    PlacedReservationImpl pr1 = createReservation(id1, handle1, "q1",
        PlacedReservation.Status.PENDING);
    
    // not there
    assertReservationStatus(restData, id1, PlacedReservation.Status.PENDING,
        false);
    assertReservationQueue(restData, id1, "q1", false);
    assertReservationNode(restData, id1, "h1", false);
    assertReservationNode(restData, id1, "h2", false);

    ClientInfo clientInfo = new ClientInfo() {
      @Override
      public UUID getClientId() {
        return UUID.randomUUID();
      }

      @Override
      public UUID getHandle() {
        return handle1;
      }

      @Override
      public String getCallbackAddress() {
        return "a:0";
      }
    };

    assertHandle(restData, handle1, false);

    //register clientInfo
    restData.onRegister(clientInfo);
    assertHandle(restData, handle1, true);

    assertReservationHandle(restData, id1, handle1, false);

    // pending
    restData.onEvent(createEvents(pr1));
    assertReservationStatus(restData, id1, PlacedReservation.Status.PENDING,
        true);
    assertReservationBackedOffFlag(restData, id1, false);
    assertResourceStatus(restData, id1, 0, PlacedResource.Status.PENDING);
    assertResourceStatus(restData, id1, 1, PlacedResource.Status.PENDING);
    assertReservationQueue(restData, id1, "q1", true);
    assertReservationHandle(restData, id1, handle1, true);
    assertReservationNode(restData, id1, "h1", true);
    assertReservationNode(restData, id1, "h2", true);

    // backed off
    pr1.setStatus(PlacedReservation.Status.BACKED_OFF);
    restData.onEvent(createEvents(pr1));
    assertReservationStatus(restData, id1, PlacedReservation.Status.BACKED_OFF,
        true);
    assertReservationBackedOffFlag(restData, id1, true);
    assertResourceStatus(restData, id1, 0, PlacedResource.Status.PENDING);
    assertResourceStatus(restData, id1, 1, PlacedResource.Status.PENDING);
    assertReservationQueue(restData, id1, "q1", true);
    assertReservationHandle(restData, id1, handle1, true);
    assertReservationNode(restData, id1, "h1", true);
    assertReservationNode(restData, id1, "h2", true);

    // pending
    pr1.setStatus(PlacedReservation.Status.PENDING);
    restData.onEvent(createEvents(pr1));;
    assertReservationStatus(restData, id1, PlacedReservation.Status.PENDING,
        true);
    assertReservationBackedOffFlag(restData, id1, true);
    assertResourceStatus(restData, id1, 0, PlacedResource.Status.PENDING);
    assertResourceStatus(restData, id1, 1, PlacedResource.Status.PENDING);
    assertReservationQueue(restData, id1, "q1", true);
    assertReservationHandle(restData, id1, handle1, true);
    assertReservationNode(restData, id1, "h1", true);
    assertReservationNode(restData, id1, "h2", true);

    // partial
    pr1.setStatus(PlacedReservation.Status.PARTIAL);
    ((PlacedResourceImpl) pr1.getResources().get(0)).setAllocationInfo("h1", 2,
        2024);
    restData.onEvent(createEvents(pr1));;
    assertReservationStatus(restData, id1, PlacedReservation.Status.PARTIAL,
        true);
    assertReservationBackedOffFlag(restData, id1, true);
    assertResourceStatus(restData, id1, 0, PlacedResource.Status.ALLOCATED);
    assertResourceStatus(restData, id1, 1, PlacedResource.Status.PENDING);
    assertReservationQueue(restData, id1, "q1", true);
    assertReservationHandle(restData, id1, handle1, true);
    assertReservationNode(restData, id1, "h1", true);
    assertReservationNode(restData, id1, "h2", true);

    // allocated
    pr1.setStatus(PlacedReservation.Status.ALLOCATED);
    ((PlacedResourceImpl) pr1.getResources().get(0)).setAllocationInfo("h1", 2,
        2024);
    ((PlacedResourceImpl) pr1.getResources().get(1)).setAllocationInfo("h3", 3,
        3036);
    restData.onEvent(createEvents(pr1));;
    assertReservationStatus(restData, id1, PlacedReservation.Status.ALLOCATED,
        true);
    assertReservationBackedOffFlag(restData, id1, true);
    assertResourceStatus(restData, id1, 0, PlacedResource.Status.ALLOCATED);
    assertResourceStatus(restData, id1, 1, PlacedResource.Status.ALLOCATED);
    assertReservationQueue(restData, id1, "q1", true);
    assertReservationHandle(restData, id1, handle1, true);
    assertReservationNode(restData, id1, "h1", true);
    assertReservationNode(restData, id1, "h3", true);

    // ended
    pr1.setStatus(PlacedReservation.Status.RELEASED);
    restData.onEvent(createEvents(pr1));;
    assertReservationStatus(restData, id1, null, false);
    assertReservationQueue(restData, id1, "q1", false);
    assertReservationHandle(restData, id1, handle1, false);
    assertReservationNode(restData, id1, "h1", false);
    assertReservationNode(restData, id1, "h3", false);
    
    // unregister handle
    restData.onUnregister(clientInfo);

    assertHandle(restData, handle1, false);

  }

  @Test
  public void testAll() throws Exception {
    RestData restData = new RestData();
    UUID id1 = UUID.randomUUID();
    final UUID handle1 = UUID.randomUUID();
    PlacedReservationImpl pr1 = createReservation(id1, handle1, "q1",
        PlacedReservation.Status.PENDING);

    ClientInfo clientInfo = new ClientInfo() {
      @Override
      public UUID getClientId() {
        return UUID.randomUUID();
      }

      @Override
      public UUID getHandle() {
        return handle1;
      }

      @Override
      public String getCallbackAddress() {
        return "a:0";
      }
    };

    restData.onRegister(clientInfo);
    restData.onEvent(createEvents(pr1));;

    StringWriter writer = new StringWriter();
    restData.writeAllAsJson(writer);
    writer.close();
    
    Map map = parseJson(writer.toString());
    Assert.assertEquals(2, map.size());
    Assert.assertTrue(map.containsKey(RestData.REST_VERSION_KEY));
    Assert.assertEquals(RestData.REST_VERSION_VALUE, 
        map.get(RestData.REST_VERSION_KEY));
    Assert.assertTrue(map.containsKey(RestData.ALL_DATA));

    map = (Map) map.get(RestData.ALL_DATA);
    Assert.assertTrue(map.containsKey(RestData.VERSION_INFO_KEY));
    assertVersionInfo((Map) map.get(RestData.VERSION_INFO_KEY));

    Map map1 = (Map) map.get(RestData.RESERVATIONS);
    Assert.assertEquals(1, map1.size());
    Assert.assertTrue(map1.containsKey(id1.toString()));
    
    List list = (List) map.get(RestData.CLIENT_INFOS);
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(handle1.toString(), ((Map)list.get(0)).get("handle"));

    map1 = (Map) map.get(RestData.QUEUES_CROSSREF);
    Assert.assertEquals(1, map1.size());
    Assert.assertTrue(map1.containsKey("q1"));
    Assert.assertEquals(id1.toString(), ((List) map1.get("q1")).get(0));

    map1 = (Map) map.get(RestData.HANDLES_CROSSREF);
    Assert.assertEquals(1, map1.size());
    Assert.assertTrue(map1.containsKey(handle1.toString()));
    Assert.assertEquals(id1.toString(), 
        ((List) map1.get(handle1.toString())).get(0));

    map1 = (Map) map.get(RestData.NODES_CROSSREF);
    Assert.assertEquals(2, map1.size());
    Assert.assertTrue(map1.containsKey("h1"));
    Assert.assertEquals(id1.toString(), ((List) map1.get("h1")).get(0));
    Assert.assertTrue(map1.containsKey("h2"));
    Assert.assertEquals(id1.toString(), ((List) map1.get("h2")).get(0));

    pr1.setStatus(PlacedReservation.Status.RELEASED);
    restData.onEvent(createEvents(pr1));;
    restData.onUnregister(clientInfo);
  }

  @Test
  public void testSummary() throws Exception {
    RestData restData = new RestData();

    StringWriter writer = new StringWriter();
    restData.writeSummaryAsJson(writer);
    writer.close();

    Map map = parseJson(writer.toString());
    Assert.assertTrue(map.containsKey(RestData.SUMMARY_DATA));
    map = (Map) map.get(RestData.SUMMARY_DATA);
    Assert.assertEquals(0, map.get(RestData.RESERVATIONS_COUNT_KEY));
    Assert.assertEquals(0,
        ((List) map.get(RestData.QUEUES_SUMMARY_KEY)).size());
    Assert.assertEquals(0,
        ((List) map.get(RestData.CLIENTS_SUMMARY_KEY)).size());
    Assert.assertEquals(0, ((List) map.get(RestData.NODES_SUMMARY_KEY)).size());
    
    UUID id1 = UUID.randomUUID();
    final UUID handle1 = UUID.randomUUID();
    PlacedReservationImpl pr1 = createReservation(id1, handle1, "q1",
        PlacedReservation.Status.PENDING);

    ClientInfo clientInfo = new ClientInfo() {
      @Override
      public UUID getClientId() {
        return UUID.randomUUID();
      }

      @Override
      public UUID getHandle() {
        return handle1;
      }

      @Override
      public String getCallbackAddress() {
        return "a:0";
      }
    };

    restData.onRegister(clientInfo);
    restData.onEvent(createEvents(pr1));;

    writer = new StringWriter();
    restData.writeSummaryAsJson(writer);
    writer.close();

    map = parseJson(writer.toString());
    Assert.assertEquals(2, map.size());
    Assert.assertTrue(map.containsKey(RestData.REST_VERSION_KEY));
    Assert.assertEquals(RestData.REST_VERSION_VALUE,
        map.get(RestData.REST_VERSION_KEY));

    Assert.assertTrue(map.containsKey(RestData.SUMMARY_DATA));
    map = (Map) map.get(RestData.SUMMARY_DATA);
    Assert.assertEquals(1, map.get(RestData.RESERVATIONS_COUNT_KEY));
    Assert.assertEquals(1, 
        ((List) map.get(RestData.QUEUES_SUMMARY_KEY)).size());
    Assert.assertEquals("q1", ((Map)
        ((List) map.get(RestData.QUEUES_SUMMARY_KEY)).get(0)).get("queue"));
    Assert.assertEquals(1, 
        ((List) map.get(RestData.CLIENTS_SUMMARY_KEY)).size());
    Assert.assertEquals(handle1.toString(), ((Map) 
        ((List) map.get(RestData.CLIENTS_SUMMARY_KEY)).get(0)).get("handle"));
    Assert.assertEquals(2, ((List) map.get(RestData.NODES_SUMMARY_KEY)).size());
    Assert.assertEquals("h1", ((Map) 
        ((List) map.get(RestData.NODES_SUMMARY_KEY)).get(0)).get("node"));
    Assert.assertEquals("h2", ((Map) 
        ((List) map.get(RestData.NODES_SUMMARY_KEY)).get(1)).get("node"));

    pr1.setStatus(PlacedReservation.Status.RELEASED);
    restData.onEvent(createEvents(pr1));;
    restData.onUnregister(clientInfo);

    writer = new StringWriter();
    restData.writeSummaryAsJson(writer);
    writer.close();

    map = parseJson(writer.toString());
    Assert.assertTrue(map.containsKey(RestData.SUMMARY_DATA));
    map = (Map) map.get(RestData.SUMMARY_DATA);
    Assert.assertEquals(0, map.get(RestData.RESERVATIONS_COUNT_KEY));
    Assert.assertEquals(0,
        ((List) map.get(RestData.QUEUES_SUMMARY_KEY)).size());
    Assert.assertEquals(0,
        ((List) map.get(RestData.CLIENTS_SUMMARY_KEY)).size());
    Assert.assertEquals(0, ((List) map.get(RestData.NODES_SUMMARY_KEY)).size());
  }

  private void assertVersionInfo(Map map) {
    Assert.assertEquals(VersionInfo.getVersion(),  map.get("llamaVersion"));
    Assert.assertEquals(VersionInfo.getBuiltDate(), map.get("llamaBuiltDate"));
    Assert.assertEquals(VersionInfo.getBuiltBy(), map.get("llamaBuiltBy"));
    Assert.assertEquals(VersionInfo.getSCMURI(), map.get("llamaScmUri"));
    Assert.assertEquals(VersionInfo.getSCMRevision(), 
        map.get("llamaScmRevision"));
    Assert.assertEquals(VersionInfo.getSourceMD5(),  map.get("llamaSourceMD5"));
    Assert.assertEquals(VersionInfo.getHadoopVersion(), 
        map.get("llamaHadoopVersion"));
  }
  
}

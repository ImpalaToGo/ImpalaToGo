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
package com.cloudera.llama.am.api;

import com.cloudera.llama.am.impl.PlacedReservationImpl;
import com.cloudera.llama.am.impl.PlacedResourceImpl;
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestUtils {

  public static Resource createResource(String location,
      Resource.Locality locality, int cpus, int memory) {
    Resource.Builder b = Builders.createResourceBuilder();
    b.setResourceId(UUID.randomUUID());
    b.setLocationAsk(location);
    b.setLocalityAsk(locality);
    b.setCpuVCoresAsk(cpus);
    b.setMemoryMbsAsk(memory);
    return b.build();
  }

  public static RMResource createRMResource(String location,
      Resource.Locality locality, int cpus, int memory) {
    return createPlacedResourceImpl(location, locality, cpus, memory);
  }

  public static PlacedResource createPlacedResource(String location,
      Resource.Locality locality, int cpus, int memory) {
    return createPlacedResourceImpl(location, locality, cpus, memory);
  }

  public static PlacedResourceImpl createPlacedResourceImpl(Resource resource) {
    Reservation rr = createReservation(UUID.randomUUID(), "u", "q",
        Arrays.asList(resource), true);
    PlacedReservationImpl pr = new PlacedReservationImpl(UUID.randomUUID(), rr);
    return pr.getPlacedResourceImpls().get(0);
  }

  public static PlacedResourceImpl createPlacedResourceImpl(String location,
      Resource.Locality locality, int cpus, int memory) {
    Resource r = createResource(location, locality, cpus, memory);
    return createPlacedResourceImpl(r);
  }

  public static Resource createResource(String location) {
    Resource.Builder b = Builders.createResourceBuilder();
    b.setResourceId(UUID.randomUUID());
    b.setLocationAsk(location);
    b.setLocalityAsk(Resource.Locality.MUST);
    b.setCpuVCoresAsk(1);
    b.setMemoryMbsAsk(2);
    return b.build();
  }

  public static Expansion createExpansion(PlacedReservation r) {
    Expansion.Builder b = Builders.createExpansionBuilder();
    b.setHandle(UUID.randomUUID());
    b.setExpansionOf(r.getReservationId());
    b.setResource(createResource("l"));
    return b.build();
  }

  public static Reservation createReservation(UUID handle, String user,
      String queue, Resource resource, boolean gang) {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setHandle(handle);
    b.setUser(user);
    b.setQueue(queue);
    b.addResources(Arrays.asList(resource));
    b.setGang(gang);
    return b.build();
  }

  public static Reservation createReservation(UUID handle, String user,
      String queue, List<Resource> resources, boolean gang) {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setHandle(handle);
    b.setUser(user);
    b.setQueue(queue);
    b.addResources(resources);
    b.setGang(gang);
    return b.build();
  }

  public static Reservation createReservation(boolean gang) {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setHandle(UUID.randomUUID());
    b.setUser("u");
    b.setQueue("q");
    b.setResources(Arrays.asList(createResource("n1")));
    b.setGang(gang);
    return b.build();
  }

  public static Reservation createReservation(UUID handle, int resources,
      boolean gang) {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setHandle(handle);
    b.setUser("u");
    b.setQueue("q");
    for (int i = 0; i < resources; i++) {
      b.addResource(createResource("n1"));
    }
    b.setGang(gang);
    return b.build();
  }

  public static Reservation createReservation(UUID handle, String queue,
      int resources, boolean gang) {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setHandle(handle);
    b.setUser("u");
    b.setQueue(queue);
    for (int i = 0; i < resources; i++) {
      b.addResource(createResource("n1"));
    }
    b.setGang(gang);
    return b.build();
  }

  public static PlacedReservation createPlacedReservation(
      Reservation reservation, PlacedReservation.Status status) {
    PlacedReservationImpl impl = new PlacedReservationImpl(UUID.randomUUID(),
        reservation);
    impl.setStatus(status);
    return impl;
  }

  public static void assertResource(Resource r1, Resource r2) {
    Assert.assertEquals(r1.getResourceId(), r2.getResourceId());
    Assert.assertEquals(r1.getLocationAsk(), r2.getLocationAsk());
    Assert.assertEquals(r1.getLocalityAsk(), r2.getLocalityAsk());
    Assert.assertEquals(r1.getCpuVCoresAsk(), r2.getCpuVCoresAsk());
    Assert.assertEquals(r2.getMemoryMbsAsk(), r2.getMemoryMbsAsk());

  }

  public static List<PlacedReservation> getReservations(
      List<LlamaAMEvent> events, PlacedReservation.Status status, boolean echo) {
    List<PlacedReservation> list = new ArrayList<PlacedReservation>();
    for (LlamaAMEvent event : events) {
      if (echo || !event.isEcho()) {
        list.addAll(getReservations(event, status));
      }
    }
    System.out.println(list);
    return list;
  }

    public static List<PlacedReservation> getReservations(LlamaAMEvent event,
      PlacedReservation.Status status) {
    List<PlacedReservation> list = new ArrayList<PlacedReservation>();
    for (PlacedReservation r : event.getReservationChanges()) {
      if (status == null || r.getStatus() == status) {
        list.add(r);
      }
    }
    return list;
  }

  public static List<PlacedResource> getResources(List<LlamaAMEvent> events,
      PlacedResource.Status status, boolean echo) {
    List<PlacedResource> list = new ArrayList<PlacedResource>();
    for (LlamaAMEvent event : events) {
      if (true || echo || !event.isEcho()) {
        list.addAll(getResources(event, status));
      }
    }
    return list;
  }

  public static List<PlacedResource> getResources(LlamaAMEvent event,
      PlacedResource.Status status) {
    List<PlacedResource> list = new ArrayList<PlacedResource>();
    for (PlacedResource r : event.getResourceChanges()) {
      if (r.getStatus() == status) {
        list.add(r);
      }
    }
    return list;
  }
}

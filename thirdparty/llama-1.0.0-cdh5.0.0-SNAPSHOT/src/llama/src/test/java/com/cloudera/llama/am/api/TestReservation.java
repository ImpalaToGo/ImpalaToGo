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

import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestReservation {

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail1() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail2() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setHandle(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail3() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setUser(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail4() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setUser("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail5() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setQueue(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail6() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setQueue("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail7() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.addResource(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail8() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.addResources(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail9() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.addResources(Arrays.asList((Resource) null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail10() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setResources(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail11() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setResources(Arrays.asList((Resource) null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail12() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setResources(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail13() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setHandle(UUID.randomUUID());
    b.setUser("u");
    b.build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail14() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setHandle(UUID.randomUUID());
    b.setQueue("q");
    b.build();
  }

  @Test
  public void testBuilderOk1() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setHandle(UUID.randomUUID());
    b.setUser("u");
    b.setQueue("q");
    b.addResource(TestUtils.createResource("n1"));
    Assert.assertNotNull(b.build());
  }

  @Test
  public void testBuilderOk2() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setHandle(UUID.randomUUID());
    b.setUser("u");
    b.setQueue("q");
    b.addResources(Arrays.asList(TestUtils.createResource("n1")));
    Assert.assertNotNull(b.build());
  }

  @Test
  public void testBuilderOk3() {
    Reservation.Builder b = Builders.createReservationBuilder();
    b.setHandle(UUID.randomUUID());
    b.setUser("u");
    b.setQueue("q");
    b.setResources(Arrays.asList(TestUtils.createResource("n1")));
    Assert.assertNotNull(b.build());
  }

  @Test
  public void testGetters1() {
    Reservation.Builder b = Builders.createReservationBuilder();
    UUID handle = UUID.randomUUID();
    b.setHandle(handle);
    b.setUser("u");
    b.setQueue("q");
    Resource resource = TestUtils.createResource("n1");
    b.setResources(Arrays.asList(resource));
    Reservation reservation = b.build();
    Assert.assertNotNull(reservation.toString());
    Assert.assertEquals(handle, reservation.getHandle());
    Assert.assertEquals("u", reservation.getUser());
    Assert.assertEquals("q", reservation.getQueue());
    Assert.assertEquals(1, reservation.getResources().size());
    TestUtils.assertResource(resource, reservation.getResources().get(0));
  }

  @Test
  public void testGetters2() {
    Reservation.Builder b = Builders.createReservationBuilder();
    UUID handle = UUID.randomUUID();
    b.setHandle(handle);
    b.setUser("u");
    b.setQueue("q");
    Resource resource1 = TestUtils.createResource("n1");
    Resource resource2 = TestUtils.createResource("n2");
    b.addResources(Arrays.asList(resource1));
    b.addResources(Arrays.asList(resource2));
    Reservation reservation = b.build();
    Assert.assertEquals(handle, reservation.getHandle());
    Assert.assertEquals("u", reservation.getUser());
    Assert.assertEquals("q", reservation.getQueue());
    Assert.assertEquals(2, reservation.getResources().size());
    TestUtils.assertResource(resource1, reservation.getResources().get(0));
    TestUtils.assertResource(resource2, reservation.getResources().get(1));
  }

  @Test
  public void testGetters3() {
    Reservation.Builder b = Builders.createReservationBuilder();
    UUID handle = UUID.randomUUID();
    b.setHandle(handle);
    b.setUser("u");
    b.setQueue("q");
    Resource resource1 = TestUtils.createResource("n1");
    Resource resource2 = TestUtils.createResource("n2");
    b.addResource(resource1);
    b.addResource(resource2);
    Reservation reservation = b.build();
    Assert.assertEquals(handle, reservation.getHandle());
    Assert.assertEquals("u", reservation.getUser());
    Assert.assertEquals("q", reservation.getQueue());
    Assert.assertEquals(2, reservation.getResources().size());
    TestUtils.assertResource(resource1, reservation.getResources().get(0));
    TestUtils.assertResource(resource2, reservation.getResources().get(1));
  }

  @Test
  public void testGetters4() {
    Reservation.Builder b = Builders.createReservationBuilder();
    UUID handle = UUID.randomUUID();
    b.setHandle(handle);
    b.setUser("u");
    b.setQueue("q");
    Resource resource1 = TestUtils.createResource("n1");
    Resource resource2 = TestUtils.createResource("n2");
    b.setResources(Arrays.asList(resource1));
    b.setResources(Arrays.asList(resource2));
    Reservation reservation = b.build();
    Assert.assertEquals(handle, reservation.getHandle());
    Assert.assertEquals("u", reservation.getUser());
    Assert.assertEquals("q", reservation.getQueue());
    Assert.assertEquals(1, reservation.getResources().size());
    TestUtils.assertResource(resource2, reservation.getResources().get(0));
  }

}

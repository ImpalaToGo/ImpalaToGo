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

public class TestExpansion {

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail1() {
    Expansion.Builder b = Builders.createExpansionBuilder();
    b.build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail2() {
    Expansion.Builder b = Builders.createExpansionBuilder();
    b.setExpansionOf(null);
    b.build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail3() {
    Expansion.Builder b = Builders.createExpansionBuilder();
    b.setExpansionOf(UUID.randomUUID());
    b.build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail4() {
    Expansion.Builder b = Builders.createExpansionBuilder();
    b.setExpansionOf(UUID.randomUUID());
    b.setResource(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail5() {
    Expansion.Builder b = Builders.createExpansionBuilder();
    b.setExpansionOf(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail6() {
    Expansion.Builder b = Builders.createExpansionBuilder();
    b.setExpansionOf(UUID.randomUUID());
    b.setResource(TestUtils.createResource("n"));
    b.build();
  }

  @Test
  public void testBuilderOk() {
    Expansion.Builder b = Builders.createExpansionBuilder();
    b.setHandle(UUID.randomUUID());
    b.setExpansionOf(UUID.randomUUID());
    b.setResource(TestUtils.createResource("n"));
    Assert.assertNotNull(b.build());
  }

  @Test
  public void testGetters1() {
    Expansion.Builder b = Builders.createExpansionBuilder();
    UUID handle = UUID.randomUUID();
    b.setHandle(handle);
    UUID reservationId = UUID.randomUUID();
    b.setExpansionOf(reservationId);
    Resource resource = TestUtils.createResource("n");
    b.setResource(resource);
    Expansion expansion = b.build();
    Assert.assertNotNull(expansion.toString());
    Assert.assertEquals(handle, expansion.getHandle());
    Assert.assertEquals(reservationId, expansion.getExpansionOf());
    TestUtils.assertResource(resource, expansion.getResource());
  }

}

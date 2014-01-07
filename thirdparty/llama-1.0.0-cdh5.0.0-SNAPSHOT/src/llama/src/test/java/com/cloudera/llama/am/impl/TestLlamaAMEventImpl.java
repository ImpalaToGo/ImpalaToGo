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
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.TestUtils;
import junit.framework.Assert;
import org.junit.Test;

public class TestLlamaAMEventImpl {

  @Test
  public void testMethods() {
    LlamaAMEventImpl event = new LlamaAMEventImpl(true);
    Assert.assertTrue(event.isEcho());
    event = new LlamaAMEventImpl();
    Assert.assertFalse(event.isEcho());
    Assert.assertTrue(event.isEmpty());
    Assert.assertTrue(event.getReservationChanges().isEmpty());
    Assert.assertTrue(event.getResourceChanges().isEmpty());

    event = new LlamaAMEventImpl();
    PlacedResource pr = TestUtils.createPlacedResourceImpl(
        TestUtils.createResource("l"));
    event.addResource(pr);
    Assert.assertFalse(event.isEmpty());
    Assert.assertTrue(event.getReservationChanges().isEmpty());
    Assert.assertFalse(event.getResourceChanges().isEmpty());
    Assert.assertEquals(1, event.getResourceChanges().size());
    Assert.assertEquals(pr, event.getResourceChanges().get(0));

    event = new LlamaAMEventImpl();
    PlacedReservation prr = TestUtils.createPlacedReservation(
        TestUtils.createReservation(true), PlacedReservation.Status.ALLOCATED);
    event.addReservation(prr);
    Assert.assertFalse(event.isEmpty());
    Assert.assertEquals(1, event.getReservationChanges().size());
    Assert.assertEquals(prr, event.getReservationChanges().get(0));
    Assert.assertTrue(event.getResourceChanges().isEmpty());

  }
}

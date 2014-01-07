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

import com.cloudera.llama.am.api.Expansion;
import com.cloudera.llama.am.api.LlamaAM;
import com.cloudera.llama.am.api.LlamaAMEvent;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

public class TestExpansionReservationsLlamaAM {

  @Test
  public void testReservationToExpansionOperations() throws Exception {
    LlamaAM am = Mockito.mock(LlamaAM.class);
    Configuration conf = new Configuration(false);
    Mockito.when(am.getConf()).thenReturn(conf);
    ExpansionReservationsLlamaAM eAm = new ExpansionReservationsLlamaAM(am);
    try {
      UUID r1 = UUID.randomUUID();
      UUID e1 = UUID.randomUUID();
      UUID e2 = UUID.randomUUID();

      Assert.assertNull(eAm.getExpansions(null));
      Assert.assertNull(eAm.getExpansions(r1));

      eAm.add(r1, e1);
      Assert.assertEquals(Arrays.asList(e1), eAm.getExpansions(r1));

      eAm.add(r1, e2);
      Assert.assertEquals(Arrays.asList(e1, e2), eAm.getExpansions(r1));

      eAm.removeExpansion(r1, e1);
      Assert.assertEquals(Arrays.asList(e2), eAm.getExpansions(r1));

      eAm.removeExpansionsOf(r1);
      Assert.assertNull(eAm.getExpansions(r1));

      eAm.add(r1, e1);
      eAm.removeExpansion(r1, e1);
      Assert.assertNull(eAm.getExpansions(r1));

    } finally {
      eAm.stop();
    }
  }

  @Test
  public void testDelegation() throws Exception {
    LlamaAM am = Mockito.mock(LlamaAM.class);
    Configuration conf = new Configuration(false);
    Mockito.when(am.getConf()).thenReturn(conf);
    ExpansionReservationsLlamaAM eAm = new ExpansionReservationsLlamaAM(am);
    try {
      eAm.setMetricRegistry(new MetricRegistry());
      Mockito.verify(am).setMetricRegistry(Mockito.any(MetricRegistry.class));
      eAm.start();
      Mockito.verify(am).start();
      eAm.getNodes();
      Mockito.verify(am).getNodes();
      eAm.isRunning();
      Mockito.verify(am).isRunning();
      UUID id = UUID.randomUUID();
      Reservation r = Mockito.mock(Reservation.class);
      eAm.reserve(id, r);
      Mockito.verify(am).reserve(id, r);
      eAm.getReservation(id);
      Mockito.verify(am).getReservation(id);
      UUID handle = UUID.randomUUID();
      eAm.releaseReservation(handle, id, false);
      Mockito.verify(am).releaseReservation(handle, id, false);
      eAm.releaseReservationsForHandle(handle, false);
      Mockito.verify(am).releaseReservationsForHandle(handle, false);
      String queue = "queue";
      eAm.releaseReservationsForQueue(queue, false);
      Mockito.verify(am).releaseReservationsForQueue(queue, false);
      eAm.emptyCacheForQueue("q");
      Mockito.verify(am).emptyCacheForQueue(Mockito.eq("q"));

    } finally {
      eAm.stop();
      Mockito.verify(am).stop();
    }
  }

  @Test(expected = LlamaException.class)
  public void testExpansionOfUnknownReservation() throws Exception {
    LlamaAM am = Mockito.mock(LlamaAM.class);
    Configuration conf = new Configuration(false);
    Mockito.when(am.getConf()).thenReturn(conf);
    ExpansionReservationsLlamaAM eAm = new ExpansionReservationsLlamaAM(am);

    Reservation r = TestUtils.createReservation(true);
    PlacedReservation pr = TestUtils.createPlacedReservation(r,
        PlacedReservation.Status.PENDING);

    try {
      Mockito.when(am.getReservation(Mockito.any(UUID.class))).thenReturn(pr);
      eAm.reserve(pr.getReservationId(), r);

      Expansion e = TestUtils.createExpansion(pr);
      eAm.expand(e);

    } finally {
      Assert.assertNull(eAm.getExpansions(pr.getReservationId()));
      eAm.stop();
    }
  }

  @Test(expected = LlamaException.class)
  public void testExpansionOfNotAllocatedReservation() throws Exception {
    LlamaAM am = Mockito.mock(LlamaAM.class);
    Configuration conf = new Configuration(false);
    Mockito.when(am.getConf()).thenReturn(conf);
    ExpansionReservationsLlamaAM eAm = new ExpansionReservationsLlamaAM(am);

    Reservation r = TestUtils.createReservation(true);
    PlacedReservation pr = TestUtils.createPlacedReservation(r,
        PlacedReservation.Status.ALLOCATED);

    try {
      Mockito.when(am.getReservation(Mockito.any(UUID.class))).thenReturn(null);

      Expansion e = TestUtils.createExpansion(pr);
      eAm.expand(e);

    } finally {
      Assert.assertNull(eAm.getExpansions(pr.getReservationId()));
      eAm.stop();
    }
  }

  @Test(expected = LlamaException.class)
  public void testExpansionOfExpansion() throws Exception {
    LlamaAM am = Mockito.mock(LlamaAM.class);
    Configuration conf = new Configuration(false);
    Mockito.when(am.getConf()).thenReturn(conf);
    ExpansionReservationsLlamaAM eAm = new ExpansionReservationsLlamaAM(am);

    Reservation r = TestUtils.createReservation(true);
    PlacedReservation pr = TestUtils.createPlacedReservation(r,
        PlacedReservation.Status.ALLOCATED);
    ((PlacedReservationImpl)pr).expansionOf = UUID.randomUUID();

    try {
      Mockito.when(am.getReservation(Mockito.any(UUID.class))).thenReturn(pr);
      eAm.reserve(pr.getReservationId(), r);

      Expansion e = TestUtils.createExpansion(pr);
      eAm.expand(e);

    } finally {
      Assert.assertNull(eAm.getExpansions(pr.getReservationId()));
      eAm.stop();
    }
  }

  @Test
  public void testExpansion() throws Exception {
    LlamaAM am = Mockito.mock(LlamaAM.class);
    Configuration conf = new Configuration(false);
    Mockito.when(am.getConf()).thenReturn(conf);
    ExpansionReservationsLlamaAM eAm = new ExpansionReservationsLlamaAM(am);
    try {
      Reservation r = TestUtils.createReservation(true);
      PlacedReservation pr = TestUtils.createPlacedReservation(r,
          PlacedReservation.Status.ALLOCATED);

      Mockito.when(am.getReservation(Mockito.any(UUID.class))).thenReturn(pr);
      eAm.reserve(pr.getReservationId(), r);

      Expansion e = TestUtils.createExpansion(pr);
      UUID eId = eAm.expand(e);
      Assert.assertNotNull(eId);

      Assert.assertEquals(Arrays.asList(eId),
          eAm.getExpansions(pr.getReservationId()));

    } finally {
      eAm.stop();
    }
  }

  @Test
  public void testReleaseExpansion() throws Exception {
    LlamaAM am = Mockito.mock(LlamaAM.class);
    Configuration conf = new Configuration(false);
    Mockito.when(am.getConf()).thenReturn(conf);
    ExpansionReservationsLlamaAM eAm = new ExpansionReservationsLlamaAM(am);
    try {
      Reservation r = TestUtils.createReservation(true);
      PlacedReservation pr = TestUtils.createPlacedReservation(r,
          PlacedReservation.Status.ALLOCATED);

      Mockito.when(am.getReservation(Mockito.any(UUID.class))).thenReturn(pr);
      eAm.reserve(pr.getReservationId(), r);

      Expansion e = TestUtils.createExpansion(pr);
      UUID eId = eAm.expand(e);
      Assert.assertNotNull(eId);

      Assert.assertEquals(Arrays.asList(eId),
          eAm.getExpansions(pr.getReservationId()));

      eAm.releaseReservation(e.getHandle(), eId, false);

      Assert.assertNull(eAm.getExpansions(pr.getReservationId()));

    } finally {
      eAm.stop();
    }
  }

  @Test
  public void testReleaseReservationWithExpansion() throws Exception {
    LlamaAM am = Mockito.mock(LlamaAM.class);
    Configuration conf = new Configuration(false);
    Mockito.when(am.getConf()).thenReturn(conf);
    ExpansionReservationsLlamaAM eAm = new ExpansionReservationsLlamaAM(am);
    try {
      Reservation r = TestUtils.createReservation(true);
      PlacedReservation pr = TestUtils.createPlacedReservation(r,
          PlacedReservation.Status.ALLOCATED);

      Mockito.when(am.getReservation(Mockito.any(UUID.class))).thenReturn(pr);
      eAm.reserve(pr.getReservationId(), r);

      Expansion e = TestUtils.createExpansion(pr);
      UUID eId = eAm.expand(e);
      Assert.assertNotNull(eId);

      Assert.assertEquals(Arrays.asList(eId),
          eAm.getExpansions(pr.getReservationId()));

      eAm.releaseReservation(pr.getHandle(), pr.getReservationId(), false);

      Mockito.verify(am).releaseReservation(Mockito.eq(LlamaAM.WILDCARD_HANDLE),
          Mockito.eq(eId), Mockito.eq(false));

      Assert.assertNull(eAm.getExpansions(pr.getReservationId()));

    } finally {
      eAm.stop();
    }
  }

  @Test
  public void testReleaseForHandleReservationWithExtensions() throws Exception {
    LlamaAM am = Mockito.mock(LlamaAM.class);
    Configuration conf = new Configuration(false);
    Mockito.when(am.getConf()).thenReturn(conf);
    ExpansionReservationsLlamaAM eAm = new ExpansionReservationsLlamaAM(am);
    try {
      Reservation r = TestUtils.createReservation(true);
      PlacedReservation pr = TestUtils.createPlacedReservation(r,
          PlacedReservation.Status.ALLOCATED);

      Mockito.when(am.getReservation(Mockito.any(UUID.class))).thenReturn(pr);
      eAm.reserve(pr.getReservationId(), r);

      Expansion e = TestUtils.createExpansion(pr);
      UUID eId = eAm.expand(e);
      Assert.assertNotNull(eId);

      Assert.assertEquals(Arrays.asList(eId),
          eAm.getExpansions(pr.getReservationId()));

      Mockito.when(am.releaseReservationsForHandle(pr.getHandle(), false)).
          thenReturn(Arrays.asList(pr));

      eAm.releaseReservationsForHandle(pr.getHandle(), false);

      Mockito.verify(am).releaseReservationsForHandle(Mockito.eq(pr.getHandle()),
          Mockito.eq(false));

      Mockito.verify(am).releaseReservation(Mockito.eq(LlamaAM.WILDCARD_HANDLE),
          Mockito.eq(eId), Mockito.eq(false));

      Assert.assertNull(eAm.getExpansions(pr.getReservationId()));

    } finally {
      eAm.stop();
    }
  }

  @Test
  public void testReleaseForQueueReservationWithExtensions() throws Exception {
    LlamaAM am = Mockito.mock(LlamaAM.class);
    Configuration conf = new Configuration(false);
    Mockito.when(am.getConf()).thenReturn(conf);
    ExpansionReservationsLlamaAM eAm = new ExpansionReservationsLlamaAM(am);
    try {
      Reservation r = TestUtils.createReservation(true);
      PlacedReservation pr = TestUtils.createPlacedReservation(r,
          PlacedReservation.Status.ALLOCATED);

      Mockito.when(am.getReservation(Mockito.any(UUID.class))).thenReturn(pr);
      eAm.reserve(pr.getReservationId(), r);

      Expansion e = TestUtils.createExpansion(pr);
      UUID eId = eAm.expand(e);
      Assert.assertNotNull(eId);

      Assert.assertEquals(Arrays.asList(eId),
          eAm.getExpansions(pr.getReservationId()));

      Mockito.when(am.releaseReservationsForQueue(pr.getQueue(), false)).
          thenReturn(Arrays.asList(pr));

      eAm.releaseReservationsForQueue(pr.getQueue(), false);

      Mockito.verify(am).releaseReservationsForQueue(Mockito.eq(pr.getQueue()),
          Mockito.eq(false));

      Assert.assertNull(eAm.getExpansions(pr.getReservationId()));

    } finally {
      eAm.stop();
    }
  }

  @Test
  public void testReleaseFromEvent() throws Exception {
    LlamaAM am = Mockito.mock(LlamaAM.class);
    Configuration conf = new Configuration(false);
    Mockito.when(am.getConf()).thenReturn(conf);
    ExpansionReservationsLlamaAM eAm = new ExpansionReservationsLlamaAM(am);
    try {
      Reservation r = TestUtils.createReservation(true);
      PlacedReservation pr = TestUtils.createPlacedReservation(r,
          PlacedReservation.Status.ALLOCATED);

      Mockito.when(am.getReservation(Mockito.any(UUID.class))).thenReturn(pr);
      eAm.reserve(pr.getReservationId(), r);

      Expansion e = TestUtils.createExpansion(pr);
      UUID eId = eAm.expand(e);
      Assert.assertNotNull(eId);

      Assert.assertEquals(Arrays.asList(eId),
          eAm.getExpansions(pr.getReservationId()));

      ((PlacedReservationImpl)pr).setStatus(PlacedReservation.Status.PREEMPTED);
      LlamaAMEvent event = LlamaAMEventImpl.createEvent(false, pr);

      eAm.onEvent(event);

      Mockito.verify(am).releaseReservation(Mockito.eq(LlamaAM.WILDCARD_HANDLE),
          Mockito.eq(eId), Mockito.eq(false));

      Assert.assertNull(eAm.getExpansions(pr.getReservationId()));

    } finally {
      eAm.stop();
    }
  }

}

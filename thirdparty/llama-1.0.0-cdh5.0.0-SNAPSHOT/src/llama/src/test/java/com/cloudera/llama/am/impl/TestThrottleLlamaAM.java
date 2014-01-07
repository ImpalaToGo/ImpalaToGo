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
import com.cloudera.llama.util.ErrorCode;
import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.util.ManualClock;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;

import java.util.Arrays;
import java.util.List;

public class TestThrottleLlamaAM {
  private ManualClock manualClock = new ManualClock();

  @Before
  public void setup() {
    Clock.setClock(manualClock);
  }

  @After
  public void cleanup() {
    Clock.setClock(Clock.SYSTEM);
  }

  @Test
  public void testStartStopConfigs() throws Exception {
        SingleQueueLlamaAM am = Mockito.mock(SingleQueueLlamaAM.class);

    //config defaults

    Configuration conf = new Configuration(false);
    ThrottleLlamaAM tAm = new ThrottleLlamaAM(conf, "q", am);
    tAm.setMetricRegistry(new MetricRegistry());
    try {
      tAm.start();
      Mockito.verify(am).start();
      Assert.assertEquals(ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_DEFAULT,
          tAm.getMaxPlacedReservations());
      Assert.assertEquals(ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_DEFAULT,
          tAm.getMaxQueuedReservations());
    } finally {
      tAm.stop();
      Mockito.verify(am).stop();
    }

    //custom global

    conf.setInt(ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_KEY,
        ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_DEFAULT + 1);
    conf.setInt(ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_KEY,
        ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_DEFAULT + 2);
    tAm = new ThrottleLlamaAM(conf, "q", am);
    try {
      tAm.start();
      Assert.assertEquals(ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_DEFAULT + 1,
          tAm.getMaxPlacedReservations());
      Assert.assertEquals(ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_DEFAULT + 2,
          tAm.getMaxQueuedReservations());

    } finally {
      tAm.stop();
    }

    //custom queue

    conf.setInt(FastFormat.format(
        ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_QUEUE_KEY, "q"), 1);
    conf.setInt(FastFormat.format(
        ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_QUEUE_KEY, "q"), 2);
    tAm = new ThrottleLlamaAM(conf, "q", am);
    try {
      tAm.start();
      Assert.assertEquals(1, tAm.getMaxPlacedReservations());
      Assert.assertEquals(2, tAm.getMaxQueuedReservations());

    } finally {
      tAm.stop();
    }
  }

  @Test
  public void testDelegation() throws Exception {
    SingleQueueLlamaAM am = Mockito.mock(SingleQueueLlamaAM.class);

    Configuration conf = new Configuration(false);
    ThrottleLlamaAM tAm = new ThrottleLlamaAM(conf, "q", am);
    try {
      tAm.start();
      tAm.emptyCacheForQueue("q");
      Mockito.verify(am).emptyCacheForQueue(Mockito.eq("q"));
      tAm.getNodes();
      Mockito.verify(am).getNodes();
      tAm.isRunning();
      Mockito.verify(am).isRunning();
    } finally {
      tAm.stop();
      Mockito.verify(am).stop();
    }
  }

    @Test
  public void testThrottleQueueMaxOut() throws Exception {
        SingleQueueLlamaAM am = Mockito.mock(SingleQueueLlamaAM.class);

    Configuration conf = new Configuration(false);
    conf.setInt(ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_KEY, 1);
    conf.setInt(ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_KEY, 2);
    ThrottleLlamaAM tAm = new ThrottleLlamaAM(conf, "q", am);
    try {
      tAm.start();
      Assert.assertEquals(0, tAm.getPlacedReservations());
      Assert.assertEquals(0, tAm.getQueuedReservations());

      Reservation r = TestUtils.createReservation(true);
      PlacedReservation pr = TestUtils.createPlacedReservation(r,
          PlacedReservation.Status.PENDING);

      Mockito.when(am.getReservation(Mockito.any(UUID.class))).thenReturn(pr);

      UUID id1 = tAm.reserve(r);
      Mockito.verify(am, VerificationModeFactory.times(1)).
          reserve(Mockito.any(UUID.class), Mockito.any(Reservation.class));
      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(0, tAm.getQueuedReservations());

      UUID id2 = tAm.reserve(r);
      Mockito.verify(am, VerificationModeFactory.times(1)).
          reserve(Mockito.any(UUID.class), Mockito.any(Reservation.class));
      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(1, tAm.getQueuedReservations());

      PlacedReservation pr2 = tAm.getReservation(id2);

      tAm.reserve(r);
      Mockito.verify(am, VerificationModeFactory.times(1)).
          reserve(Mockito.any(UUID.class), Mockito.any(Reservation.class));
      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(2, tAm.getQueuedReservations());

      Assert.assertEquals(pr, tAm.getReservation(id1));
      Mockito.verify(am).getReservation(Mockito.eq(id1));
      Assert.assertEquals(pr2, tAm.getReservation(id2));

      try {
        tAm.reserve(r);
        Assert.fail();
      } catch (LlamaException ex) {
        Assert.assertEquals(
            ErrorCode.LLAMA_MAX_RESERVATIONS_FOR_QUEUE.getCode(),
            ex.getErrorCode());
      } catch (Throwable ex) {
        Assert.fail();
      }

      Assert.assertEquals(2, tAm.getQueuedReservations());

    } finally {
      tAm.stop();
    }
  }

  @Test
  public void testReleasePlaced() throws Exception {
    Reservation r = TestUtils.createReservation(true);
    PlacedReservation pr = TestUtils.createPlacedReservation(r,
        PlacedReservation.Status.PENDING);
        SingleQueueLlamaAM am = Mockito.mock(SingleQueueLlamaAM.class);

    Mockito.when(am.releaseReservation(Mockito.eq(pr.getHandle()),
        Mockito.eq(pr.getReservationId()), Mockito.eq(false))).thenReturn(pr);

    Configuration conf = new Configuration(false);
    conf.setInt(ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_KEY, 1);
    conf.setInt(ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_KEY, 2);
    ThrottleLlamaAM tAm = new ThrottleLlamaAM(conf, "q", am);
    try {
      UUID id = pr.getReservationId();
      tAm.start();
      tAm.reserve(id, r);
      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(0, tAm.getQueuedReservations());
      tAm.releaseReservation(pr.getHandle(), id, false);
      Mockito.verify(am, VerificationModeFactory.times(1)).
          releaseReservation(Mockito.any(UUID.class), Mockito.any(UUID.class),
              Mockito.anyBoolean());
      //simulation underlying AM release event
      ((PlacedReservationImpl)pr).setStatus(PlacedReservation.Status.RELEASED);
      tAm.onEvent(LlamaAMEventImpl.createEvent(true, pr));
      Assert.assertEquals(0, tAm.getPlacedReservations());
      Assert.assertEquals(0, tAm.getQueuedReservations());

      //forcing the no reservation path
      tAm.releaseReservation(pr.getHandle(), id, false);

    } finally {
      tAm.stop();
    }
  }

  @Test
  public void testReleaseQueued() throws Exception {
    Reservation r1 = TestUtils.createReservation(true);
    Reservation r2 = TestUtils.createReservation(true);
    PlacedReservation pr1 = TestUtils.createPlacedReservation(r1,
        PlacedReservation.Status.PENDING);
    PlacedReservation pr2 = TestUtils.createPlacedReservation(r2,
        PlacedReservation.Status.PENDING);

    SingleQueueLlamaAM am = Mockito.mock(SingleQueueLlamaAM.class);

    Mockito.when(am.releaseReservation(Mockito.eq(pr1.getHandle()),
        Mockito.eq(pr1.getReservationId()), Mockito.eq(false))).thenReturn(pr1);

    Configuration conf = new Configuration(false);
    conf.setInt(ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_KEY, 1);
    conf.setInt(ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_KEY, 2);
    ThrottleLlamaAM tAm = new ThrottleLlamaAM(conf, "q", am);
    try {
      tAm.start();
      tAm.reserve(pr1.getReservationId(), r1);
      tAm.reserve(pr2.getReservationId(), r2);
      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(1, tAm.getQueuedReservations());
      tAm.releaseReservation(pr2.getHandle(), pr2.getReservationId(), false);
      Mockito.verify(am, VerificationModeFactory.times(0)).
          releaseReservation(Mockito.any(UUID.class), Mockito.any(UUID.class),
              Mockito.anyBoolean());
      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(0, tAm.getQueuedReservations());

      //forcing the no reservation path
      tAm.releaseReservation(pr2.getHandle(), pr2.getReservationId(), false);

    } finally {
      tAm.stop();
    }
  }

  @Test
  public void testPlaceFromQueueWhenPlacedReleased() throws Exception {
    Reservation r1 = TestUtils.createReservation(true);
    Reservation r2 = TestUtils.createReservation(true);
    PlacedReservation pr1 = TestUtils.createPlacedReservation(r1,
        PlacedReservation.Status.PENDING);
    PlacedReservation pr2 = TestUtils.createPlacedReservation(r2,
        PlacedReservation.Status.PENDING);

    SingleQueueLlamaAM am = Mockito.mock(SingleQueueLlamaAM.class);

    Mockito.when(am.releaseReservation(Mockito.eq(pr1.getHandle()),
        Mockito.eq(pr1.getReservationId()), Mockito.eq(false))).thenReturn(pr1);

    Configuration conf = new Configuration(false);
    conf.setInt(ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_KEY, 1);
    conf.setInt(ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_KEY, 2);
    ThrottleLlamaAM tAm = new ThrottleLlamaAM(conf, "q", am);
    try {
      tAm.start();
      tAm.reserve(pr1.getReservationId(), r1);
      tAm.reserve(pr2.getReservationId(), r2);
      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(1, tAm.getQueuedReservations());
      tAm.releaseReservation(pr1.getHandle(), pr1.getReservationId(), false);
      //simulation underlying AM release event
      ((PlacedReservationImpl) pr1).setStatus(PlacedReservation.Status.RELEASED);
      tAm.onEvent(LlamaAMEventImpl.createEvent(true, pr1));
      manualClock.increment(1001);
      Thread.sleep(100);

      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(0, tAm.getQueuedReservations());
      Mockito.verify(am, VerificationModeFactory.times(2)).
          reserve(Mockito.any(UUID.class), Mockito.any(Reservation.class));
    } finally {
      tAm.stop();
    }
  }

  @Test
  public void testPlaceFromQueueWhenPlacedEndsBecauseOfRM() throws Exception {
    Reservation r1 = TestUtils.createReservation(true);
    Reservation r2 = TestUtils.createReservation(true);
    PlacedReservation pr1 = TestUtils.createPlacedReservation(r1,
        PlacedReservation.Status.PENDING);
    PlacedReservation pr2 = TestUtils.createPlacedReservation(r2,
        PlacedReservation.Status.PENDING);

    SingleQueueLlamaAM am = Mockito.mock(SingleQueueLlamaAM.class);

    Mockito.when(am.releaseReservation(Mockito.eq(pr1.getHandle()),
        Mockito.eq(pr1.getReservationId()), Mockito.eq(false))).thenReturn(pr1);

    Configuration conf = new Configuration(false);
    conf.setInt(ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_KEY, 1);
    conf.setInt(ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_KEY, 2);
    ThrottleLlamaAM tAm = new ThrottleLlamaAM(conf, "q", am);
    try {
      tAm.start();
      tAm.reserve(pr1.getReservationId(), r1);
      tAm.reserve(pr2.getReservationId(), r2);
      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(1, tAm.getQueuedReservations());

      LlamaAMEventImpl event = new LlamaAMEventImpl();
      ((PlacedReservationImpl)pr1).setStatus(PlacedReservation.Status.ALLOCATED);
      event.addReservation(pr1);
      tAm.onEvent(event);
      manualClock.increment(1001);
      Thread.sleep(100);

      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(1, tAm.getQueuedReservations());

      event = new LlamaAMEventImpl();
      ((PlacedReservationImpl) pr1).setStatus(PlacedReservation.Status.PREEMPTED);
      event.addReservation(pr1);
      tAm.onEvent(event);
      manualClock.increment(1001);
      Thread.sleep(100);

      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(0, tAm.getQueuedReservations());
      Mockito.verify(am, VerificationModeFactory.times(2)).
          reserve(Mockito.any(UUID.class), Mockito.any(Reservation.class));
    } finally {
      tAm.stop();
    }
  }

  @Test
  public void testReleaseForHandle() throws Exception {
    Reservation r1 = TestUtils.createReservation(true);
    Reservation r2 = TestUtils.createReservation(true);
    PlacedReservation pr1 = TestUtils.createPlacedReservation(r1,
        PlacedReservation.Status.PENDING);
    PlacedReservation pr2 = TestUtils.createPlacedReservation(r2,
        PlacedReservation.Status.PENDING);

    UUID handle = UUID.randomUUID();
    Reservation r3 = TestUtils.createReservation(handle, 1, false);
    Reservation r4 = TestUtils.createReservation(handle, 1, false);
    PlacedReservation pr3 = TestUtils.createPlacedReservation(r3,
        PlacedReservation.Status.PENDING);
    PlacedReservation pr4 = TestUtils.createPlacedReservation(r4,
        PlacedReservation.Status.PENDING);

    SingleQueueLlamaAM am = Mockito.mock(SingleQueueLlamaAM.class);

    Mockito.when(am.releaseReservation(Mockito.eq(pr1.getHandle()),
        Mockito.eq(pr1.getReservationId()), Mockito.eq(false))).thenReturn(pr1);

    Mockito.when(am.releaseReservationsForHandle(Mockito.eq(pr3.getHandle()),
        Mockito.eq(false))).thenReturn(Arrays.asList(pr3));

    //config defaults

    Configuration conf = new Configuration(false);
    conf.setInt(ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_KEY, 2);
    conf.setInt(ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_KEY, 2);
    ThrottleLlamaAM tAm = new ThrottleLlamaAM(conf, "q", am);
    try {
      tAm.start();
      tAm.reserve(pr1.getReservationId(), r1);
      tAm.reserve(pr3.getReservationId(), r3);
      tAm.reserve(pr2.getReservationId(), r2);
      tAm.reserve(pr4.getReservationId(), r4);
      Assert.assertEquals(2, tAm.getPlacedReservations());
      Assert.assertEquals(2, tAm.getQueuedReservations());
      List<PlacedReservation> released =
          tAm.releaseReservationsForHandle(handle, false);
      //simulation underlying AM release event
      ((PlacedReservationImpl) pr3).setStatus(PlacedReservation.Status.RELEASED);
      tAm.onEvent(LlamaAMEventImpl.createEvent(true, pr3));

      Assert.assertTrue(released.contains(pr3));
      Assert.assertTrue(released.contains(pr4));
      manualClock.increment(1001);
      Thread.sleep(100);
      Assert.assertEquals(2, tAm.getPlacedReservations());
      Assert.assertEquals(0, tAm.getQueuedReservations());
      Mockito.verify(am, VerificationModeFactory.times(3)).
          reserve(Mockito.any(UUID.class), Mockito.any(Reservation.class));
    } finally {
      tAm.stop();
    }
  }

  @Test
  public void testReleaseForQueue() throws Exception {
    Reservation r1 = TestUtils.createReservation(true);
    Reservation r2 = TestUtils.createReservation(true);
    PlacedReservation pr1 = TestUtils.createPlacedReservation(r1,
        PlacedReservation.Status.PENDING);
    PlacedReservation pr2 = TestUtils.createPlacedReservation(r2,
        PlacedReservation.Status.PENDING);

        SingleQueueLlamaAM am = Mockito.mock(SingleQueueLlamaAM.class);

    Mockito.when(am.releaseReservationsForQueue(Mockito.anyString(),
        Mockito.eq(false))).thenReturn(Arrays.asList(pr1));

    Configuration conf = new Configuration(false);
    conf.setInt(ThrottleLlamaAM.MAX_PLACED_RESERVATIONS_KEY, 1);
    conf.setInt(ThrottleLlamaAM.MAX_QUEUED_RESERVATIONS_KEY, 1);
    ThrottleLlamaAM tAm = new ThrottleLlamaAM(conf, "q", am);
    try {
      tAm.start();
      tAm.reserve(pr1.getReservationId(), r1);
      tAm.reserve(pr2.getReservationId(), r2);
      Assert.assertEquals(1, tAm.getPlacedReservations());
      Assert.assertEquals(1, tAm.getQueuedReservations());
      List<PlacedReservation> released =
          tAm.releaseReservationsForQueue("q", false);
      //simulation underlying AM release event
      ((PlacedReservationImpl) pr1).setStatus(PlacedReservation.Status.RELEASED);
      tAm.onEvent(LlamaAMEventImpl.createEvent(true, pr1));
      Assert.assertEquals(2, released.size());
      Assert.assertTrue(released.contains(pr1));
      Assert.assertTrue(released.contains(pr2));
      manualClock.increment(1001);
      Thread.sleep(100);
      Assert.assertEquals(0, tAm.getPlacedReservations());
      Assert.assertEquals(0, tAm.getQueuedReservations());
      Mockito.verify(am, VerificationModeFactory.times(1)).
          reserve(Mockito.any(UUID.class), Mockito.any(Reservation.class));

    } finally {
      tAm.stop();
    }

  }

}

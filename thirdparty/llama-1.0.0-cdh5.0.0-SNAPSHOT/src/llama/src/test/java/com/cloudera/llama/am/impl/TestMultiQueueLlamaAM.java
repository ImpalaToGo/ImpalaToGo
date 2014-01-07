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
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.ErrorCode;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.util.ManualClock;
import com.cloudera.llama.am.api.LlamaAMListener;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.am.spi.RMConnector;
import com.cloudera.llama.am.spi.RMEvent;
import com.cloudera.llama.util.UUID;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class TestMultiQueueLlamaAM {

  private static List<String> EXPECTED = Arrays.asList("setConf",
      "setLlamaAMCallback", "start", "register", "reserve", "release",
      "unregister", "stop");

  private static MyRMConnector rmConnector;
  
  public static class MyRMConnector extends RecordingMockRMConnector
      implements Configurable {
    private Configuration conf;

    public MyRMConnector() {
      rmConnector = this;
    }

    @Override
    public void setConf(Configuration conf) {
      invoked.add("setConf");
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return null;
    }

    @Override
    public void start() throws LlamaException {
      super.start();
      if (conf.getBoolean("fail.start", false)) {
        throw new LlamaException(ErrorCode.TEST);
      }
    }

    @Override
    public void register(String queue) throws LlamaException {
      super.register(queue);
      if (conf.getBoolean("fail.register", false)) {
        throw new LlamaException(ErrorCode.TEST);
      }
    }

    @Override
    public void release(Collection<RMResource> resources,
        boolean doNotCache)
        throws LlamaException {
      super.release(resources, doNotCache);
      if (conf.getBoolean("release.fail", false)) {
        throw new LlamaException(ErrorCode.TEST);
      }
    }

  }
  
  @After
  public void tearDown() {
    Clock.setClock(Clock.SYSTEM);
  }

  @Test
  public void testMultiQueueDelegation() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setClass(LlamaAM.RM_CONNECTOR_CLASS_KEY, MyRMConnector.class,
        RMConnector.class);
    LlamaAM am = LlamaAM.create(conf);
    try {
      am.start();
      LlamaAMListener listener = new LlamaAMListener() {
        @Override
        public void onEvent(LlamaAMEvent event) {
        }
      };
      UUID handle = UUID.randomUUID();
      UUID id = am.reserve(TestUtils.createReservation(handle, "q", 1, true));
      am.getNodes();
      am.addListener(listener);
      am.removeListener(listener);
      am.getReservation(id);
      am.releaseReservation(handle, id, false);
      am.releaseReservationsForHandle(UUID.randomUUID(), false);
      am.stop();

      Assert.assertEquals(EXPECTED, rmConnector.invoked);
    } finally {
      am.stop();
    }
  }

  @Test(expected = LlamaException.class)
  public void testReleaseReservationForClientException() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setClass(LlamaAM.RM_CONNECTOR_CLASS_KEY, MyRMConnector.class,
        RMConnector.class);
    conf.setBoolean("release.fail", true);
    LlamaAM am = LlamaAM.create(conf);
    try {
      am.start();
      UUID cId = UUID.randomUUID();
      am.reserve(TestUtils.createReservation(cId, "q", 1, true));
      am.releaseReservationsForHandle(cId, false);
    } finally {
      am.stop();
    }
  }

  @Test(expected = LlamaException.class)
  public void testReleaseReservationForClientDiffQueuesException()
      throws Exception {
    Configuration conf = new Configuration(false);
    conf.setClass(LlamaAM.RM_CONNECTOR_CLASS_KEY, MyRMConnector.class,
        RMConnector.class);
    conf.setBoolean("release.fail", true);
    LlamaAM am = LlamaAM.create(conf);
    try {
      am.start();
      UUID cId = UUID.randomUUID();
      am.reserve(TestUtils.createReservation(cId, "q1", 1, true));
      am.reserve(TestUtils.createReservation(cId, "q2", 1, true));
      am.releaseReservationsForHandle(cId, false);
    } finally {
      am.stop();
    }
  }

  @Test(expected = LlamaException.class)
  public void testStartOfDelegatedLlamaAmFail() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setClass(LlamaAM.RM_CONNECTOR_CLASS_KEY, MyRMConnector.class,
        RMConnector.class);
    conf.setBoolean("fail.start", true);
    conf.set(LlamaAM.CORE_QUEUES_KEY, "q");
    LlamaAM am = LlamaAM.create(conf);
    am.start();
  }

  @Test(expected = LlamaException.class)
  public void testRegisterOfDelegatedLlamaAmFail() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setClass(LlamaAM.RM_CONNECTOR_CLASS_KEY, MyRMConnector.class,
        RMConnector.class);
    conf.setBoolean("fail.register", true);
    conf.set(LlamaAM.CORE_QUEUES_KEY, "q");
    LlamaAM am = LlamaAM.create(conf);
    am.start();
  }

  @Test
  public void testGetReservationUnknown() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setClass(LlamaAM.RM_CONNECTOR_CLASS_KEY, MyRMConnector.class,
        RMConnector.class);
    LlamaAM am = LlamaAM.create(conf);
    am.start();
    Assert.assertNull(am.getReservation(UUID.randomUUID()));
  }

  @Test
  public void testReleaseReservationUnknown() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setClass(LlamaAM.RM_CONNECTOR_CLASS_KEY, MyRMConnector.class,
        RMConnector.class);
    LlamaAM am = LlamaAM.create(conf);
    am.start();
    am.releaseReservation(UUID.randomUUID(), UUID.randomUUID(), false);
  }

  private boolean listenerCalled;

  @SuppressWarnings("unchecked")
  @Test
  public void testMultiQueueListener() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setClass(LlamaAM.RM_CONNECTOR_CLASS_KEY, MyRMConnector.class,
        RMConnector.class);
    LlamaAM am = LlamaAM.create(conf);
    try {
      am.start();
      LlamaAMListener listener = new LlamaAMListener() {
        @Override
        public void onEvent(LlamaAMEvent event) {
          listenerCalled = true;
        }
      };
      UUID handle = UUID.randomUUID();
      PlacedReservation rr = am.getReservation(
          am.reserve(TestUtils.createReservation(handle,
          "q", 1, true)));
      UUID id = rr.getReservationId();
      am.getNodes();
      am.addListener(listener);
      am.getReservation(id);
      Assert.assertFalse(listenerCalled);
      List<RMResource> resources = (List<RMResource>) rmConnector.args.get(3);
      rmConnector.callback.onEvent(Arrays.asList(RMEvent
          .createStatusChangeEvent(resources.get(0).getResourceId(),
              PlacedResource.Status.REJECTED)));
      Assert.assertTrue(listenerCalled);
      am.releaseReservation(handle, id, false);
      am.releaseReservationsForHandle(UUID.randomUUID(), false);
      am.removeListener(listener);
      listenerCalled = false;
      Assert.assertFalse(listenerCalled);
      am.stop();
    } finally {
      am.stop();
    }
  }

  @Test
  public void testQueueExpiry() throws Exception {
    ManualClock clock = new ManualClock();
    Clock.setClock(clock);
    Configuration conf = new Configuration(false);
    conf.setClass(LlamaAM.RM_CONNECTOR_CLASS_KEY, MyRMConnector.class,
        RMConnector.class);
    conf.set(LlamaAM.CORE_QUEUES_KEY, "root.corequeue");
    MultiQueueLlamaAM am = new MultiQueueLlamaAM(conf);
    am.amCheckExpiryIntervalMs = 20;
    am.start();

    // Core queue should exist
    Assert.assertEquals(1, am.ams.keySet().size());

    UUID handle = UUID.randomUUID();
    UUID resId = am.reserve(TestUtils.createReservation(handle, "root.someotherqueue", 1, true));
    Assert.assertEquals(2, am.ams.keySet().size());
    am.releaseReservation(handle, resId, true);
    clock.increment(LlamaAM.QUEUE_AM_EXPIRE_MS_DEFAULT * 2);

    Thread.sleep(300); // am expiry check should run in this time
    // Other queue should get cleaned up
    Assert.assertEquals(1, am.ams.keySet().size());

    handle = UUID.randomUUID();
    resId = am.reserve(TestUtils.createReservation(handle, "root.corequeue", 1, true));
    am.releaseReservation(handle, resId, true);
    clock.increment(LlamaAM.QUEUE_AM_EXPIRE_MS_DEFAULT * 2);

    Thread.sleep(300); // am expiry check should run in this time
    // Core queue should still exist
    Assert.assertEquals(1, am.ams.keySet().size());
  }
}

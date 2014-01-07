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

import com.cloudera.llama.am.api.LlamaAMEvent;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.am.api.LlamaAMListener;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.am.api.Resource;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.List;

public class TestLlamaAMImpl {

  public class MyLlamaAMImpl extends LlamaAMImpl {
    protected MyLlamaAMImpl(Configuration conf) {
      super(conf);
    }

    @Override
    public void start() throws LlamaException {
    }

    @Override
    public void stop() {
    }

    @Override
    public boolean isRunning() {
      return false;
    }

    @Override
    public List<String> getNodes() throws LlamaException {
      return null;
    }

    @Override
    public void reserve(UUID reservationId,
        Reservation reservation)
        throws LlamaException {
    }

    @Override
    public PlacedReservation getReservation(UUID reservationId)
        throws LlamaException {
      return null;
    }

    @Override
    public PlacedReservation releaseReservation(UUID handle, UUID reservationId,
        boolean doNotCache)
        throws LlamaException {
      return null;
    }

    @Override
    public List<PlacedReservation> releaseReservationsForHandle(UUID handle,
        boolean doNotCache)
        throws LlamaException {
      return null;
    }

    @Override
    public List<PlacedReservation> releaseReservationsForQueue(String queue,
        boolean doNotCache)
        throws LlamaException {
      return null;
    }

    @Override
    public void emptyCacheForQueue(String queue) throws LlamaException {
    }

  }

  public class MyListener implements LlamaAMListener {
    LlamaAMEvent event;
    boolean throwEx;

    @Override
    public void onEvent(LlamaAMEvent event) {
      this.event = event;
      if (throwEx) {
        throw new RuntimeException();
      }
    }
  }

  @Test
  public void testListeners() {
    MyListener listener = new MyListener();
    LlamaAMImpl am = new MyLlamaAMImpl(new Configuration(false));
    am.addListener(listener);
    LlamaAMEventImpl event = new LlamaAMEventImpl();
    am.dispatch(event);
    Assert.assertNull(listener.event);
    event.addResource(TestUtils.createPlacedResource("l",
        Resource.Locality.DONT_CARE, 1, 1));
    am.dispatch(event);
    Assert.assertEquals(event, listener.event);
    listener.event = null;

    event.addReservation(TestUtils.createPlacedReservation(
        TestUtils.createReservation(true), PlacedReservation.Status.ALLOCATED));
    am.dispatch(event);
    Assert.assertEquals(event, listener.event);
    listener.event = null;
    listener.throwEx = true;
    am.dispatch(event);
    Assert.assertEquals(event, listener.event);
    listener.event = null;
    am.removeListener(listener);
    am.dispatch(event);
    Assert.assertNull(listener.event);
    am.dispatch(event);
    Assert.assertNull(listener.event);
  }

}

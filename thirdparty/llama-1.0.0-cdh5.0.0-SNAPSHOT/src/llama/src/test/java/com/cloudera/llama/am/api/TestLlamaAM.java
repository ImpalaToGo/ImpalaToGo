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

import com.cloudera.llama.am.spi.RMConnector;
import com.cloudera.llama.am.spi.RMListener;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

public class TestLlamaAM {

  public static class MyRMConnector implements RMConnector {
    static boolean created;

    public MyRMConnector() {
      created = true;
    }

    @Override
    public void setLlamaAMCallback(RMListener callback) {
    }

    @Override
    public void start() throws LlamaException {
    }

    @Override
    public void stop() {
    }

    @Override
    public void register(String queue) throws LlamaException {
    }

    @Override
    public void unregister() {
    }

    @Override
    public List<String> getNodes() throws LlamaException {
      return null;
    }

    @Override
    public void reserve(Collection<RMResource> resources)
        throws LlamaException {
    }

    @Override
    public void release(Collection<RMResource> resources, boolean doNotCache)
        throws LlamaException {
    }

    @Override
    public boolean reassignResource(Object rmResourceId, UUID resourceId) {
      return false;
    }

    public void emptyCache() throws LlamaException {
    }

    @Override
    public void setMetricRegistry(MetricRegistry registry) {
    }
  }

  private void testCreate(Configuration conf) throws Exception {
    LlamaAM am = LlamaAM.create(conf);
    try {
      am.start();
      am.reserve(TestUtils.createReservation(true));
      Assert.assertTrue(MyRMConnector.created);
    } finally {
      am.stop();
    }
  }

  @Test
  public void testCreate() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setClass(LlamaAM.RM_CONNECTOR_CLASS_KEY, MyRMConnector.class,
        RMConnector.class);
    testCreate(conf);
  }

}

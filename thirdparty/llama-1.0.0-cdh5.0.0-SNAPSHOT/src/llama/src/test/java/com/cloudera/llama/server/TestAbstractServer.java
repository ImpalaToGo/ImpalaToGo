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
package com.cloudera.llama.server;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import javax.security.auth.Subject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class TestAbstractServer {
  private static final Set<String> STEPS = new HashSet<String>();

  static {
    STEPS.add("metrics");
    STEPS.add("jmx");
    STEPS.add("service");
    STEPS.add("transport");
  }

  public static class MyServer extends AbstractServer {
    private volatile boolean started = false;

    protected MyServer() {
      super("myServer");
    }

    private void trace(String propName) {
      getConf().setBoolean(propName + ".trace", true);
    }

    private void failIf(String propName) {
      if (getConf().getBoolean(propName + ".fail", false)) {
        throw new RuntimeException();
      }
    }

    @Override
    protected void startMetrics() {
      trace("start.metrics");
      failIf("start.metrics");
    }

    @Override
    protected void stopMetrics() {
      trace("stop.metrics");
      failIf("stop.metrics");
    }

    @Override
    protected void startJMX() {
      trace("start.jmx");
      failIf("start.jmx");
    }

    @Override
    protected void stopJMX() {
      trace("stop.jmx");
      failIf("stop.jmx");
    }

    @Override
    protected Subject loginServerSubject() {
      return new Subject();
    }

    @Override
    protected void startService() {
      trace("start.service");
      failIf("start.service");
    }

    @Override
    protected void stopService() {
      trace("stop.service");
      failIf("stop.service");
    }

    @Override
    protected void startTransport(CountDownLatch latch) {
      latch.countDown();
      trace("start.transport");
      failIf("start.transport");
      started = true;
    }

    @Override
    protected void stopTransport() {
      trace("stop.transport");
      failIf("stop.transport");
    }

    @Override
    public String getAddressHost() {
      return "localhost";
    }

    @Override
    public int getAddressPort() {
      return (started) ? 1 : 0;
    }
  }

  @Test
  public void testStartStopStepsOK() throws Exception {
    MyServer server = new MyServer();
    Configuration conf = new Configuration(false);
    server.setConf(conf);
    server.start();
    for (String step : STEPS) {
      Assert.assertTrue(conf.getBoolean("start." + step + ".trace", false));
      Assert.assertFalse(conf.getBoolean("stop." + step + ".trace", false));
    }
    for (String step : STEPS) {
      conf.setBoolean("start." + step + ".trace", false);
    }
    server.stop();
    for (String step : STEPS) {
      Assert.assertFalse(conf.getBoolean("start." + step + ".trace", false));
      Assert.assertTrue(conf.getBoolean("stop." + step + ".trace", false));
    }
  }

  @Test(expected = RuntimeException.class)
  public void testDoubleStartFailure() throws Exception {
    MyServer server = new MyServer();
    try {
      Configuration conf = new Configuration(false);
      server.setConf(conf);
      server.start();
      server.start();
    } finally {
      server.stop();
    }
  }

  @Test
  public void testStartStepsFailures() throws Exception {
    for (String step : STEPS) {
      MyServer server = new MyServer();
      Configuration conf = new Configuration(false);
      conf.setBoolean("start." + step + ".fail", true);
      server.setConf(conf);
      try {
        server.start();
        Assert.fail();
      } catch (RuntimeException ex) {
        //NOP        
      } catch (Exception ex) {
        Assert.fail();
      } finally {
        server.stop();
      }
    }
  }

  @Test
  public void testStopStepsFailures() throws Exception {
    for (String step : STEPS) {
      MyServer server = new MyServer();
      Configuration conf = new Configuration(false);
      conf.setBoolean("stop." + step + ".fail", true);
      server.setConf(conf);
      server.start();
      server.stop();
      for (String step2 : STEPS) {
        Assert.assertTrue(conf.getBoolean("start." + step2 + ".trace", false));
        Assert.assertTrue(conf.getBoolean("stop." + step2 + ".trace", false));
      }
    }
  }

  @Test
  public void testShutdown() throws Exception {
    final MyServer server = new MyServer();
    Configuration conf = new Configuration(false);
    server.setConf(conf);
    server.start();
    server.shutdown(1);
    Assert.assertEquals(1, server.getExitCode());
  }

}

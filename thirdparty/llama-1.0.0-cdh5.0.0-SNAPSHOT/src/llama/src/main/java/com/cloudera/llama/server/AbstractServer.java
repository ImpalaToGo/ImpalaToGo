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

import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractServer implements Configurable {
  private static final Logger LOG = 
      LoggerFactory.getLogger(AbstractServer.class);

  private final String serverName;
  private Configuration llamaConf;
  private Subject serverSubject;
  private int runLevel = 0;
  private int exitCode = 0;

  protected AbstractServer(String serverName) {
    this.serverName = serverName;
  }

  @Override
  public void setConf(Configuration conf) {
    llamaConf = conf;
  }

  @Override
  public Configuration getConf() {
    return llamaConf;
  }

  private volatile Exception transportException = null;

  protected abstract Subject loginServerSubject();

  protected Subject getServerSubject() {
    return serverSubject;
  }

  // non blocking
  public synchronized void start() {
    if (runLevel != 0) {
      throw new RuntimeException("AbstractServer already started");
    }
    serverSubject = loginServerSubject();
    runLevel = 0;
    LOG.trace("Starting metrics");
    startMetrics();
    runLevel = 1;
    LOG.trace("Starting JMX");
    startJMX();
    runLevel = 2;
    LOG.trace("Starting service '{}'", serverName);
    startService();
    runLevel = 3;
    LOG.trace("Starting transport");

    final CountDownLatch transportLatch = new CountDownLatch(1);
    Thread transportThread = new Thread("llama-transport-server") {
      @Override
      public void run() {
        try {
          startTransport(transportLatch);
        } catch (Exception ex) {
          transportException = ex;
          LOG.error(ex.toString(), ex);
        }
      }
    };
    transportThread.start();
    try {
      transportLatch.await();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    final CountDownLatch adminLatch = new CountDownLatch(1);
    Thread adminTransportThread = new Thread("llama-admin-transport-server") {
      @Override
      public void run() {
        try {
          startAdminTransport(adminLatch);
        } catch (Exception ex) {
          transportException = ex;
          LOG.error(ex.toString(), ex);
        }
      }
    };
    adminTransportThread.start();
    try {
      adminLatch.await();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    while (getAddressPort() == 0) {
      if (transportException != null) {
        stop();
        throw new RuntimeException(transportException);
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
    runLevel = 4;
    LOG.info("Server listening at '{}:{}'", getAddressHost(),
        getAddressPort());
    LOG.info("Llama started!");
  }

  public void shutdown(int exitCode) {
    this.exitCode = exitCode;
    LOG.debug("Initiating shutdown");
    stop();
  }

  public int getExitCode() {
    return exitCode;
  }

  public void stop() {
    if (runLevel >= 4) {
      try {
        LOG.trace("Stopping transport");
        stopTransport();
      } catch (Throwable ex) {
        LOG.warn("Failed to stop transport server: {}", ex.toString(), ex);
      }
    }
    if (runLevel >= 3) {
      try {
        LOG.trace("Stopping service '{}'", serverName);
        stopService();
      } catch (Throwable ex) {
        LOG.warn("Failed to stop service '{}': {}", serverName,
            ex.toString(), ex);
      }
    }
    if (runLevel >= 2) {
      try {
        LOG.trace("Stopping JMX");
        stopJMX();
      } catch (Throwable ex) {
        LOG.warn("Failed to stop JMX: {}", ex.toString(), ex);
      }
    }
    if (runLevel >= 1) {
      try {
        LOG.trace("Stopping metrics");
        stopMetrics();
      } catch (Throwable ex) {
        LOG.warn("Failed to stop Metrics: {}", ex.toString(), ex);
      }
    }
    if (serverSubject != null) {
      Security.logout(serverSubject);
    }
    LOG.info("Llama shutdown!");
    runLevel = -1;
  }

  private MetricRegistry metrics = new MetricRegistry();

  protected void startMetrics() {
    metrics = new MetricRegistry();
  }

  protected MetricRegistry getMetricRegistry() {
    return metrics;
  }

  protected void stopMetrics() {
    metrics = null;
  }

  protected void startJMX() {
  }

  protected void stopJMX() {
  }

  protected abstract void startService();

  protected abstract void stopService();

  //blocking
  protected abstract void startTransport(CountDownLatch latch);

  protected void startAdminTransport(CountDownLatch latch) {
    latch.countDown();
  }

  protected abstract void stopTransport();

  public abstract String getAddressHost();

  public abstract int getAddressPort();

  public String getAdminAddressHost() {
    return null;
  }

  public int getAdminAddressPort() {
    return -1;
  }

}

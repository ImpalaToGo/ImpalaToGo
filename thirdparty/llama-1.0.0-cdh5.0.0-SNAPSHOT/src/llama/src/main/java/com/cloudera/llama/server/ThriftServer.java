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

import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.util.ThriftThreadPoolExecutor;
import com.codahale.metrics.Gauge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;

import javax.security.auth.Subject;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class ThriftServer<T extends TProcessor, A extends TProcessor>
    extends AbstractServer {

  private Class<? extends ServerConfiguration> serverConfClass;
  private ServerConfiguration sConf;
  private TServer tServer;
  private TServerSocket tServerSocket;
  private TServer tAdminServer;
  private TServerSocket tAdminServerSocket;
  private String hostname;
  private int port;
  private String adminHostname;
  private int adminPort;

  protected ThriftServer(String serviceName,
      Class<? extends ServerConfiguration> serverConfClass) {
    super(serviceName);
    this.serverConfClass = serverConfClass;
  }

  protected ServerConfiguration getServerConf() {
    return sConf;
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf.get(ServerConfiguration.CONFIG_DIR_KEY) == null) {
      throw new RuntimeException(FastFormat.format(
          "Required configuration property '{}' missing",
          ServerConfiguration.CONFIG_DIR_KEY));
    }
    super.setConf(conf);
    sConf = ReflectionUtils.newInstance(serverConfClass, conf);
  }

  ThreadPoolExecutor createExecutorService(String name, int minThreads,
      int maxThreads) {
    final ThreadPoolExecutor executor = new ThriftThreadPoolExecutor(name,
        minThreads, maxThreads);
    executor.prestartAllCoreThreads();
    if (getMetricRegistry() != null) {
      getMetricRegistry().register("llama.am." + name + ".active.threads.gauge",
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return executor.getActiveCount();
            }
          });
      getMetricRegistry().register("llama.am." + name + ".total.threads.gauge",
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return executor.getPoolSize();
            }
          });
    }
    return executor;
  }

  @Override
  protected Subject loginServerSubject() {
    try {
      return Security.loginServerSubject(sConf);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected void startTransport(final CountDownLatch latch) {
    try {
      Subject.doAs(getServerSubject(), new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          int minThreads = sConf.getServerMinThreads();
          int maxThreads = sConf.getServerMaxThreads();
          tServerSocket = ThriftEndPoint.createTServerSocket(sConf);
          TTransportFactory tTransportFactory = ThriftEndPoint
              .createTTransportFactory(sConf);
          TProcessor processor = createServiceProcessor();
          processor = ThriftEndPoint.createTProcessorWrapper(sConf, false,
              processor);
          TThreadPoolServer.Args args = new TThreadPoolServer.Args
              (tServerSocket);
          args.executorService(createExecutorService("llama-thrift", minThreads,
              maxThreads));
          args.transportFactory(tTransportFactory);          
          args.processor(processor);
          tServer = new TThreadPoolServer(args);
          latch.countDown();
          tServer.serve();
          return null;
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void startAdminTransport(final CountDownLatch latch) {
    final TProcessor processor = createAdminServiceProcessor();
    if (processor != null) {
      try {
        Subject.doAs(getServerSubject(), new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            int minThreads = 1;
            int maxThreads = 10;
            tAdminServerSocket = ThriftEndPoint.createAdminTServerSocket(sConf);
            TTransportFactory tTransportFactory = ThriftEndPoint
                .createTTransportFactory(sConf);
            TProcessor tProcessor = ThriftEndPoint.createTProcessorWrapper(
                sConf, true, processor);
            TThreadPoolServer.Args args = new TThreadPoolServer.Args
                (tAdminServerSocket);
            args.executorService(createExecutorService("llama-thrift-admin",
                minThreads, maxThreads));
            args.transportFactory(tTransportFactory);
            args.processor(tProcessor);
            tAdminServer = new TThreadPoolServer(args);
            latch.countDown();
            tAdminServer.serve();
            return null;
          }
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      latch.countDown();
    }
  }

  @Override
  protected void stopTransport() {
    tServer.stop();
  }

  @Override
  public synchronized String getAddressHost() {
    if (hostname == null) {
      hostname = (tServerSocket != null &&
          tServerSocket.getServerSocket().isBound())
                 ? getHostname(tServerSocket.getServerSocket().
          getInetAddress().getHostName())
                 : null;
    }
    return hostname;
  }

  @Override
  public synchronized int getAddressPort() {
    if (port == 0) {
      port = (tServerSocket != null &&
          tServerSocket.getServerSocket().isBound())
             ? tServerSocket.getServerSocket().getLocalPort() : 0;
    }
    return port;
  }

  @Override
  public synchronized String getAdminAddressHost() {
    if (adminHostname == null) {
      adminHostname = (tAdminServerSocket != null &&
          tAdminServerSocket.getServerSocket().isBound())
                 ? getHostname(tAdminServerSocket.getServerSocket().
          getInetAddress().getHostName())
                 : null;
    }
    return adminHostname;
  }

  @Override
  public synchronized int getAdminAddressPort() {
    if (adminPort == 0) {
      adminPort = (tAdminServerSocket != null &&
          tAdminServerSocket.getServerSocket().isBound())
             ? tAdminServerSocket.getServerSocket().getLocalPort() : 0;
    }
    return adminPort;
  }

  protected abstract T createServiceProcessor();

  protected A createAdminServiceProcessor() {
    return null;
  }

  public static String getHostname(String address) {
    try {
      if (address.startsWith("0.0.0.0")) {
        address = InetAddress.getLocalHost().getCanonicalHostName();
      } else {
        int i = address.indexOf(":");
        if (i > -1) {
          address = address.substring(0, i);
        }
      }
      return address;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}

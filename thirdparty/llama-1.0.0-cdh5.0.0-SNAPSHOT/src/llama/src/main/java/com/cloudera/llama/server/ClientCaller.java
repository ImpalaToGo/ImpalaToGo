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
import com.cloudera.llama.thrift.LlamaNotificationService;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

public class ClientCaller {
  private final ServerConfiguration conf;
  private final UUID clientId;
  private final UUID handle;
  private final String host;
  private final int port;
  private final MetricRegistry metricRegistry;
  private TTransport tTransport;
  private LlamaNotificationService.Iface client;
  private boolean lastSuccessful;
  private long lastCall;

  public ClientCaller(ServerConfiguration conf, UUID clientId, UUID handle,
      String host, int port, MetricRegistry metricRegistry) {
    this.conf = conf;
    this.clientId = clientId;
    this.handle = handle;
    this.host = host;
    this.port = port;
    lastCall = System.currentTimeMillis();
    this.metricRegistry = metricRegistry;
  }

  public UUID getClientId() {
    return clientId;
  }

  public UUID getHandle() {
    return handle;
  }

  public static abstract class Callable<T> implements
      java.util.concurrent.Callable<T> {
    private UUID clientId;
    private UUID handle;
    private LlamaNotificationService.Iface client;

    protected UUID getHandle() {
      return handle;
    }

    protected UUID getClientId() {
      return clientId;
    }

    protected LlamaNotificationService.Iface getClient() {
      return client;
    }

    public abstract T call() throws ClientException;
  }

  public synchronized <T> T execute(Callable<T> callable)
      throws ClientException {
    T ret;
    try {
      lastCall = System.currentTimeMillis();
      if (!lastSuccessful) {
        client = createClient();
      }
      callable.clientId = clientId;
      callable.handle = handle;
      callable.client = client;
      ret = callable.call();
      lastSuccessful = true;
    } catch (Exception ex) {
      lastSuccessful = false;
      throw new ClientException(FastFormat.format(
          "Could not connect to '{}:{}', {}", host, port, ex), ex);
    }
    return ret;
  }

  LlamaNotificationService.Iface createClient() throws Exception {
    tTransport = ThriftEndPoint.createClientTransport(conf, host, port);
    tTransport.open();
    TProtocol protocol = new TBinaryProtocol(tTransport);
    LlamaNotificationService.Iface client =
        new LlamaNotificationService.Client(protocol);
    client = new MetricClientLlamaNotificationService(client, metricRegistry);
    return client;
  }

  public synchronized void cleanUpClient() {
    if (tTransport != null) {
      tTransport.close();
    }
    tTransport = null;
    client = null;
  }

  public synchronized long getLastCall() {
    return lastCall;
  }

}

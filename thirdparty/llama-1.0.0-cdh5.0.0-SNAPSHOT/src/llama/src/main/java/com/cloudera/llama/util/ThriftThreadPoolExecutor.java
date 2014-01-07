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
package com.cloudera.llama.util;

import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThriftThreadPoolExecutor extends ThreadPoolExecutor
    implements RejectedExecutionHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(ThriftThreadPoolExecutor.class);

  private static Field clientTTransportField;
  private static Field clientTTransportSocketField;

  static {
    //IMPORTANT: this has been tested with Thrift 0.9.0 only
    try {
      Class klass = Thread.currentThread().getContextClassLoader().
          loadClass(TThreadPoolServer.class.getName() + "$WorkerProcess");
      clientTTransportField = klass.getDeclaredField("client_");
      clientTTransportField.setAccessible(true);
      clientTTransportSocketField = TSocket.class.getDeclaredField("socket_");
      clientTTransportSocketField.setAccessible(true);
    } catch (Exception ex) {
      throw new RuntimeException(
          "Could not setup for rejected clients cleanup handling: " +
              ex.toString(), ex);
    }
  }

  private final int maxThreads;
  private final Map<Runnable, Thread> running;

  public ThriftThreadPoolExecutor(String name, int minThreads, int maxThreads) {
    super(minThreads, maxThreads, 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new NamedThreadFactory(name));
    this.maxThreads = maxThreads;
    running = new HashMap<Runnable, Thread>();
    setRejectedExecutionHandler(this);
  }

  //IMPORTANT: this has been tested with Thrift 0.9.0 only
  // by doing this we make the rejected client get an exception instead of hanging
  private String disposeClient(Runnable r) {
    String ret = "?";
    try {
      TTransport transport = (TTransport) clientTTransportField.get(r);
      if (transport instanceof TSocket) {
        Object socket = clientTTransportSocketField.get(transport);
        ret = socket.toString();
      }
      transport.close();
    } catch (Exception ex) {
      LOG.warn("Could not clean up rejected client '{}', {}", ret,
          ex.toString(), ex);
    }
    return ret;
  }

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    String client = disposeClient(r);
    LOG.error("Discarding new incoming client '{}', using maximum threads '{}'",
        client, maxThreads);
  }

  @Override
  protected synchronized void beforeExecute(Thread t, Runnable r) {
    running.put(r, t);
  }

  @Override
  protected synchronized void afterExecute(Runnable r, Throwable t) {
    running.remove(r);
    if (t instanceof InterruptedException) {
      String client = disposeClient(r);
      LOG.warn("Llama admin disposed client '{}'", client);
    }
  }

  public synchronized void interruptAllThreads() {
    for (Map.Entry<Runnable, Thread> entry : running.entrySet()) {
      entry.getValue().interrupt();
    }
  }

}

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

import com.cloudera.llama.am.api.LlamaAMEvent;
import com.cloudera.llama.am.api.LlamaAMListener;
import com.cloudera.llama.util.ErrorCode;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.util.ParamChecker;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ClientNotificationService implements ClientNotifier.ClientRegistry,
    LlamaAMListener, Gauge<Integer> {

  private static final String METRIC_PREFIX = "llama.server.";

  private static final String CLIENTS_GAUGE = METRIC_PREFIX + "clients.gauge";
  private static final String CLIENTS_EVICTION_METER = METRIC_PREFIX +
      "clients-eviction.meter";

  public static final List<String> METRIC_KEYS = Arrays.asList(
      CLIENTS_GAUGE, CLIENTS_EVICTION_METER);

  public interface Listener {

    public void onRegister(ClientInfo clientInfo);

    public void onUnregister(ClientInfo clientInfo);

  }

  private class Entry implements ClientInfo {
    private final UUID clientId;
    private final UUID handle;
    private final String host;
    private final int port;
    private final String address;
    private final ClientCaller caller;

    public Entry(UUID clientId, UUID handle, String host, int port) {
      this.clientId = clientId;
      this.handle = handle;
      this.host = host;
      this.port = port;
      this.address = host + ":" + port;
      caller = new ClientCaller(conf, clientId, handle, host, port,
          metricRegistry);
    }

    @Override
    public UUID getClientId() {
      return clientId;
    }

    @Override
    public UUID getHandle() {
      return handle;
    }

    @Override
    public String getCallbackAddress() {
      return address;
    }

  }

  private final ServerConfiguration conf;
  private final ClientNotifier clientNotifier;
  private final ReentrantReadWriteLock lock;
  //MAP of handle to client Entry
  private final ConcurrentHashMap<UUID, Entry> clients;
  //Map of clientId to handle (for reverse lookup)
  private final ConcurrentHashMap<UUID, UUID> clientIdToHandle;
  //Map of callback-address to handle (for reverse lookup)
  private final ConcurrentHashMap<String, UUID> callbackToHandle;
  private final MetricRegistry metricRegistry;
  private final List<Listener> listeners;

  public ClientNotificationService(ServerConfiguration conf,
      NodeMapper nodeMapper, MetricRegistry metricRegistry) {
    this.conf = conf;
    lock = new ReentrantReadWriteLock();
    clients = new ConcurrentHashMap<UUID, Entry>();
    clientIdToHandle = new ConcurrentHashMap<UUID, UUID>();
    callbackToHandle = new ConcurrentHashMap<String, UUID>();
    this.metricRegistry = metricRegistry;
    if (metricRegistry != null) {
      MetricClientLlamaNotificationService.registerMetric(metricRegistry);
      metricRegistry.register(CLIENTS_GAUGE, this);
      MetricUtil.registerMeter(metricRegistry, CLIENTS_EVICTION_METER);
      ClientNotifier.registerMetric(metricRegistry);
    }
    clientNotifier = new ClientNotifier(conf, nodeMapper, this, metricRegistry);
    listeners = new ArrayList<Listener>();
  }

  public void addListener(Listener listener) {
    listeners.add(ParamChecker.notNull(listener, "listener"));
  }

  public void removeListener(Listener listener) {
    listeners.remove(ParamChecker.notNull(listener, "listener"));
  }

  public void start() throws Exception {
    clientNotifier.start();
  }

  public void stop() {
    clientNotifier.stop();
  }

  private String getAddress(String host, int port) {
    return host.toLowerCase() + ":" + port;
  }
  private UUID registerNewClient(UUID clientId, String host, int port) {
    UUID handle = UUID.randomUUID();
    clients.put(handle, new Entry(clientId, handle, host, port));
    clientIdToHandle.put(clientId, handle);
    callbackToHandle.put(getAddress(host, port), handle);
    clientNotifier.registerClientForHeartbeats(handle);
    return handle;
  }


  public synchronized UUID register(UUID clientId, String host, int port)
      throws LlamaException {
    lock.writeLock().lock();
    try {
      UUID handle;
      UUID clientIdHandle = clientIdToHandle.get(clientId);
      UUID callbackHandle = callbackToHandle.get(getAddress(host, port));
      if (clientIdHandle == null && callbackHandle == null) {
        //NEW HANDLE
        handle = registerNewClient(clientId, host, port);
      } else if (clientIdHandle == null) {
        //NEW HANDLE, delete reservations from old handle
        unregister(callbackHandle);
        handle = registerNewClient(clientId, host, port);
      } else if (callbackHandle == null) {
        //ERROR
        Entry entry = clients.get(clientIdHandle);
        throw new LlamaException(ErrorCode.CLIENT_REGISTERED_WITH_OTHER_CALLBACK,
            clientId, getAddress(entry.host, entry.port));
      } else if (clientIdHandle == callbackHandle) {
        handle = clientIdHandle;
      } else {
        //ERROR
        throw new LlamaException(ErrorCode.CLIENT_INVALID_REGISTRATION, clientId,
            getAddress(host, port), clientIdHandle, callbackHandle);
      }
      Entry entry = clients.get(handle);
      for (Listener listener : listeners) {
        try {
          listener.onRegister(entry);
        } catch (Throwable ex) {
          //TODO LOG ERROR
        }
      }
      return handle;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public synchronized boolean unregister(UUID handle) {
    boolean ret = false;
    lock.writeLock().lock();
    try {
      Entry entry = clients.remove(handle);
      if (entry != null) {
        entry.caller.cleanUpClient();
        clientIdToHandle.remove(entry.clientId);
        callbackToHandle.remove(getAddress(entry.host, entry.port));
        for (Listener listener : listeners) {
          try {
            listener.onUnregister(entry);
          } catch (Throwable ex) {
            //TODO LOG ERROR
          }
        }
        ret = true;
      }
    } finally {
      lock.writeLock().unlock();
    }
    return ret;
  }

  public void validateHandle(UUID handle) throws LlamaException {
    lock.readLock().lock();
    try {
      if (!clients.containsKey(handle)) {
        throw new LlamaException(ErrorCode.CLIENT_UNKNOWN_HANDLE, handle);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public ClientCaller getClientCaller(UUID handle) {
    lock.readLock().lock();
    try {
      Entry entry = clients.get(handle);
      return (entry != null) ? entry.caller : null;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void onMaxFailures(UUID handle) {
    MetricUtil.meter(metricRegistry, CLIENTS_EVICTION_METER, 1);
    unregister(handle);
  }

  @Override
  public void onEvent(LlamaAMEvent event) {
    clientNotifier.onEvent(event);
  }

  //metric

  @Override
  public Integer getValue() {
    return clients.size();
  }

}

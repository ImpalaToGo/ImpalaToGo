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
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.PlacedResource;
import com.cloudera.llama.thrift.TLlamaAMNotificationRequest;
import com.cloudera.llama.thrift.TLlamaAMNotificationResponse;
import com.cloudera.llama.util.DelayedRunnable;
import com.cloudera.llama.util.NamedThreadFactory;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ClientNotifier implements LlamaAMListener {
  private static final Logger LOG = LoggerFactory.getLogger(
      ClientNotifier.class);

  private static final String METRIC_PREFIX =
      "llama.server.thrift-outgoing.";

  private static final String NOTIFICATION_FAILURES_METER = METRIC_PREFIX +
      "Notification.failures.meter";

  public static final List<String> METRIC_KEYS = Arrays.asList(
      NOTIFICATION_FAILURES_METER);

  public static void registerMetric(MetricRegistry metricRegistry) {
    if (metricRegistry != null) {
      MetricUtil.registerMeter(metricRegistry, NOTIFICATION_FAILURES_METER);
    }
  }

  public interface ClientRegistry {
    
    public ClientCaller getClientCaller(UUID handle);

    public void onMaxFailures(UUID handle);
  }

  private final ServerConfiguration conf;
  private final NodeMapper nodeMapper;
  private final ClientRegistry clientRegistry;
  private final MetricRegistry metricRegistry;
  private int queueThreshold;
  private int maxRetries;
  private int retryInverval;
  private int clientHeartbeat;
  private DelayQueue<DelayedRunnable> eventsQueue;
  private ThreadPoolExecutor executor;
  private Subject subject;

  public ClientNotifier(ServerConfiguration conf, NodeMapper nodeMapper,
      ClientRegistry clientRegistry, MetricRegistry metricRegistry) {
    this.conf = conf;
    this.nodeMapper = nodeMapper;
    this.clientRegistry = clientRegistry;
    this.metricRegistry = metricRegistry;
    queueThreshold = conf.getClientNotifierQueueThreshold();
    maxRetries = conf.getClientNotifierMaxRetries();
    retryInverval = conf.getClientNotifierRetryInterval();
    clientHeartbeat = conf.getClientNotifierHeartbeat();
  }

  @SuppressWarnings("unchecked")
  public void start() throws Exception {
    eventsQueue = new DelayQueue<DelayedRunnable>();
    int threads = conf.getClientNotifierThreads();
    //funny downcasting and upcasting because javac gets goofy here
    executor = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS,
        (BlockingQueue<Runnable>) (BlockingQueue) eventsQueue,
        new NamedThreadFactory("llama-notifier"));
    executor.prestartAllCoreThreads();
    subject = Security.loginClientSubject(conf);
  }

  public void stop() {
    executor.shutdownNow();
    Security.logout(subject);
  }

  private void queueNotifier(Notifier notifier) {
    eventsQueue.add(notifier);
    int size = eventsQueue.size();
    if (size > queueThreshold) {
      LOG.warn("Outbound events queue over '{}' threshold at '{}'",
          queueThreshold, size);
    }
  }

  public void registerClientForHeartbeats(UUID handle) {
    queueNotifier(new Notifier(handle));
  }

  @Override
  public void onEvent(LlamaAMEvent event) {
    if (!event.isEcho()) {
      if (nodeMapper == null) {
        throw new IllegalStateException("Cannot handle LlamaAMEvents without a" +
            "NodeMapper implementation");
      }
      Map<UUID, List<Object>> mapRR =
          new HashMap<UUID, List<Object>>();
      for (PlacedReservation rr : event.getReservationChanges()) {
        List<Object> list = mapRR.get(rr.getHandle());
        if (list == null) {
          list = new ArrayList<Object>();
          mapRR.put(rr.getHandle(), list);
        }
        list.add(rr);
      }
      for (PlacedResource r : event.getResourceChanges()) {
        List<Object> list = mapRR.get(r.getHandle());
        if (list == null) {
          list = new ArrayList<Object>();
          mapRR.put(r.getHandle(), list);
        }
        list.add(r);
      }
      for (Map.Entry<UUID, List<Object>> entry : mapRR.entrySet()) {
        queueNotifier(new Notifier(entry.getKey(),
            TypeUtils.toAMNotification(entry.getKey(), entry.getValue(),
                nodeMapper)));

      }
    }
  }

  private void notify(final ClientCaller clientCaller,
      final TLlamaAMNotificationRequest request)
      throws Exception {
    Subject.doAs(subject, new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        clientCaller.execute(new ClientCaller.Callable<Void>() {
          @Override
          public Void call() throws ClientException {
            try {
              TLlamaAMNotificationResponse response =
                  getClient().AMNotification(request);
              if (!TypeUtils.isOK(response.getStatus())) {
                LOG.warn("Client notification rejected status '{}', " +
                    "reason: {}", response.getStatus().getStatus_code(),
                    response.getStatus().getError_msgs());
              }
            } catch (TException ex) {
              throw new ClientException(ex);
            }
            return null;
          }
        });
        return null;
      }
    });
  }

  public class Notifier extends DelayedRunnable {
    private UUID handle;
    private TLlamaAMNotificationRequest notification;
    private int retries;

    public Notifier(UUID handle) {
      super(clientHeartbeat);
      this.handle = handle;
      this.notification = null;
      retries = 0;
    }

    public Notifier(UUID handle, TLlamaAMNotificationRequest notification) {
      super(0);
      this.handle = handle;
      this.notification = notification;
      retries = 0;
    }

    protected void doNotification(ClientCaller clientCaller) throws Exception {
      boolean success = false;
      if (notification != null) {
        LOG.debug("Doing notification for clientId '{}'",
            clientCaller.getClientId());
        ClientNotifier.this.notify(clientCaller, notification);
        success = true;
      } else {
        long lastCall = System.currentTimeMillis() - clientCaller.getLastCall();
        if (lastCall > clientHeartbeat) {
          LOG.debug("Doing heartbeat for clientId '{}'",
              clientCaller.getClientId());
          TLlamaAMNotificationRequest request = TypeUtils.createHearbeat(handle);
          ClientNotifier.this.notify(clientCaller, request);
          success = true;
          setDelay(clientHeartbeat);
        } else {
          LOG.debug("Skipping heartbeat for clientId '{}'",
              clientCaller.getClientId());
          setDelay(clientHeartbeat - lastCall);
        }
        ClientNotifier.this.eventsQueue.add(this);
      }
      if (success && retries > 0) {
        LOG.warn("Notification to '{}' successful after '{}' retries, " +
            "resetting retry counter for client", clientCaller.getClientId(),
            retries);
        retries = 0;
      }
    }

    @Override
    public void run() {
      UUID clientId = null;
      try {
        ClientCaller clientCaller = clientRegistry.getClientCaller(handle);
        if (clientCaller != null) {
          clientId = clientCaller.getClientId();
          doNotification(clientCaller);
        } else {
          LOG.warn("Handle '{}' not known, client notification discarded",
              handle);
        }
      } catch (Exception ex) {
        MetricUtil.meter(metricRegistry, NOTIFICATION_FAILURES_METER, 1);
        if (retries < maxRetries) {
          retries++;
          LOG.warn("Notification to '{}' failed '{}' time(s), " +
              "retrying in " + "'{}' ms, error: {}", clientId, retries,
              retryInverval, ex.toString(), ex);
          setDelay(retryInverval);
          eventsQueue.add(this);
        } else {
          LOG.warn("Notification to '{}' retried '{}' time(s), releasing " +
              "client, error: {}", clientId, retries, ex.toString(), ex);
          clientRegistry.onMaxFailures(handle);
        }
      }
    }
  }

}

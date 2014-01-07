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

import com.cloudera.llama.thrift.LlamaNotificationService;
import com.cloudera.llama.thrift.TLlamaAMNotificationRequest;
import com.cloudera.llama.thrift.TLlamaAMNotificationResponse;
import com.cloudera.llama.thrift.TLlamaNMNotificationRequest;
import com.cloudera.llama.thrift.TLlamaNMNotificationResponse;
import com.cloudera.llama.thrift.TStatus;
import com.cloudera.llama.thrift.TUniqueId;
import com.codahale.metrics.MetricRegistry;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.List;

public class MetricClientLlamaNotificationService implements
    LlamaNotificationService.Iface {
  private static final String METRIC_PREFIX =
      "llama.server.thrift-outgoing.";

  private static final String NOTIFICATION_TIMER = METRIC_PREFIX +
      "Notification.timer";

  private static final String NOTIFICATION_METER = METRIC_PREFIX +
      "Notification.meter";

  public static final List<String> METRIC_KEYS = Arrays.asList(
      NOTIFICATION_TIMER, NOTIFICATION_METER);

  public static void registerMetric(MetricRegistry metricRegistry) {
    if (metricRegistry != null) {
      MetricUtil.registerTimer(metricRegistry, NOTIFICATION_TIMER);
      MetricUtil.registerMeter(metricRegistry, NOTIFICATION_METER);
    }
  }

  private static final String CLIENT_HANDLE_MSG = "client-handle:{}";
  private static final String CALL_FAILED = "call failed";

  private static class LogContext extends MetricUtil.LogContext {
    public LogContext(TStatus status, TUniqueId handle) {
      super(TypeUtils.OK.equals(status) ? CLIENT_HANDLE_MSG : CALL_FAILED,
          TypeUtils.OK.equals(status) ? handle : null);
    }
  }

  private LlamaNotificationService.Iface client;
  private final MetricRegistry metricRegistry;

  public MetricClientLlamaNotificationService(LlamaNotificationService.Iface
      client, MetricRegistry metricRegistry) {
    this.client = client;
    this.metricRegistry = metricRegistry;
  }

  @Override
  public TLlamaAMNotificationResponse AMNotification(
      TLlamaAMNotificationRequest request) throws TException {
    TLlamaAMNotificationResponse response = null;
    long time = System.currentTimeMillis();
    try {
      response = client.AMNotification(request);
      return response;
    } finally {
      time = System.currentTimeMillis() - time;
      Object logContext = (response != null) ?
                          new LogContext(response.getStatus(), request.getAm_handle())
                                             : null;
      MetricUtil.time(metricRegistry, NOTIFICATION_TIMER, time, logContext);
      MetricUtil.meter(metricRegistry, NOTIFICATION_METER, 1);
    }
  }

  @Override
  public TLlamaNMNotificationResponse NMNotification(
      TLlamaNMNotificationRequest request) throws TException {
    TLlamaNMNotificationResponse response = null;
    long time = System.currentTimeMillis();
    try {
      response = client.NMNotification(request);
      return response;
    } finally {
      time = System.currentTimeMillis() - time;
      Object logContext =
          (response != null)
          ? new LogContext(response.getStatus(), request.getNm_handle()) : null;
      MetricUtil.time(metricRegistry, NOTIFICATION_TIMER, time, logContext);
      MetricUtil.meter(metricRegistry, NOTIFICATION_METER, 1);
    }
  }

}

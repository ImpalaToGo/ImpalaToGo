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
package com.cloudera.llama.am;

import com.cloudera.llama.server.MetricUtil;
import com.cloudera.llama.server.TypeUtils;
import com.cloudera.llama.thrift.LlamaAMService;
import com.cloudera.llama.thrift.TLlamaAMGetNodesRequest;
import com.cloudera.llama.thrift.TLlamaAMGetNodesResponse;
import com.cloudera.llama.thrift.TLlamaAMRegisterRequest;
import com.cloudera.llama.thrift.TLlamaAMRegisterResponse;
import com.cloudera.llama.thrift.TLlamaAMReleaseRequest;
import com.cloudera.llama.thrift.TLlamaAMReleaseResponse;
import com.cloudera.llama.thrift.TLlamaAMReservationExpansionRequest;
import com.cloudera.llama.thrift.TLlamaAMReservationExpansionResponse;
import com.cloudera.llama.thrift.TLlamaAMReservationRequest;
import com.cloudera.llama.thrift.TLlamaAMReservationResponse;
import com.cloudera.llama.thrift.TLlamaAMUnregisterRequest;
import com.cloudera.llama.thrift.TLlamaAMUnregisterResponse;
import com.cloudera.llama.thrift.TStatus;
import com.cloudera.llama.thrift.TUniqueId;
import com.codahale.metrics.MetricRegistry;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.List;

public class MetricLlamaAMService implements LlamaAMService.Iface {
  private static final String METRIC_PREFIX = "llama.server.thrift-incoming.";

  private static final String REGISTER_TIMER = METRIC_PREFIX + "Register.timer";
  private static final String UNREGISTER_TIMER = METRIC_PREFIX + "Unregister.timer";
  private static final String RESERVE_TIMER = METRIC_PREFIX + "Reserve.timer";
  private static final String EXPAND_TIMER = METRIC_PREFIX + "Expand.timer";
  private static final String RELEASE_TIMER = METRIC_PREFIX + "Release.timer";
  private static final String GET_NODES_TIMER = METRIC_PREFIX + "GetNodes.timer";
  private static final String RESERVE_METER = METRIC_PREFIX + "Reserve.meter";
  private static final String EXPAND_METER = METRIC_PREFIX + "Expand.meter";
  private static final String RELEASE_METER = METRIC_PREFIX + "Release.meter";

  public static final List<String> METRIC_KEYS = Arrays.asList(
      REGISTER_TIMER, UNREGISTER_TIMER, RESERVE_TIMER, RELEASE_TIMER,
      GET_NODES_TIMER, RESERVE_METER, RELEASE_METER);

  public static void registerMetric(MetricRegistry metricRegistry) {
    if (metricRegistry != null) {
      MetricUtil.registerTimer(metricRegistry, REGISTER_TIMER);
      MetricUtil.registerTimer(metricRegistry, UNREGISTER_TIMER);
      MetricUtil.registerTimer(metricRegistry, RESERVE_TIMER);
      MetricUtil.registerTimer(metricRegistry, RELEASE_TIMER);
      MetricUtil.registerTimer(metricRegistry, GET_NODES_TIMER);
      MetricUtil.registerMeter(metricRegistry, RESERVE_METER);
      MetricUtil.registerMeter(metricRegistry, RELEASE_METER);
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

  private final LlamaAMService.Iface service;
  private final MetricRegistry metricRegistry;

  public MetricLlamaAMService(LlamaAMService.Iface service,
      MetricRegistry metricRegistry) {
    this.service = service;
    this.metricRegistry = metricRegistry;
  }

  @Override
  public TLlamaAMRegisterResponse Register(TLlamaAMRegisterRequest request)
      throws TException {
    TLlamaAMRegisterResponse response = null;
    long time = System.currentTimeMillis();
    try {
      response = service.Register(request);
      return response;
    } finally {
      time = System.currentTimeMillis() - time;
      Object logContext = (response != null) ?
        new LogContext(response.getStatus(), response.getAm_handle()) : null;
      MetricUtil.time(metricRegistry, REGISTER_TIMER, time, logContext);
    }
  }

  @Override
  public TLlamaAMUnregisterResponse Unregister(
      TLlamaAMUnregisterRequest request) throws TException {
    TLlamaAMUnregisterResponse response = null;
    long time = System.currentTimeMillis();
    try {
      response = service.Unregister(request);
      return response;
    } finally {
      time = System.currentTimeMillis() - time;
      Object logContext = (response != null) ?
        new LogContext(response.getStatus(), request.getAm_handle()) : null;
      MetricUtil.time(metricRegistry, UNREGISTER_TIMER, time, logContext);
    }
  }

  @Override
  public TLlamaAMReservationResponse Reserve(TLlamaAMReservationRequest request)
      throws TException {
    TLlamaAMReservationResponse response = null;
    long time = System.currentTimeMillis();
    try {
      response = service.Reserve(request);
      return response;
    } finally {
      time = System.currentTimeMillis() - time;
      Object logContext = (response != null) ?
        new LogContext(response.getStatus(), request.getAm_handle()) : null;
      MetricUtil.time(metricRegistry, RESERVE_TIMER, time, logContext);
      MetricUtil.meter(metricRegistry, RESERVE_METER, 1);
    }
  }


  @Override
  public TLlamaAMReservationExpansionResponse Expand(
      TLlamaAMReservationExpansionRequest request) throws TException {
    TLlamaAMReservationExpansionResponse response = null;
    long time = System.currentTimeMillis();
    try {
      response = service.Expand(request);
      return response;
    } finally {
      time = System.currentTimeMillis() - time;
      Object logContext = (response != null) ?
        new LogContext(response.getStatus(), request.getAm_handle()) : null;
      MetricUtil.time(metricRegistry, EXPAND_TIMER, time, logContext);
      MetricUtil.meter(metricRegistry, EXPAND_METER, 1);
    }
  }

  @Override
  public TLlamaAMReleaseResponse Release(TLlamaAMReleaseRequest request)
      throws TException {
    TLlamaAMReleaseResponse response = null;
    long time = System.currentTimeMillis();
    try {
      response = service.Release(request);
      return response;
    } finally {
      time = System.currentTimeMillis() - time;
      Object logContext = (response != null) ?
        new LogContext(response.getStatus(), request.getAm_handle()) : null;
      MetricUtil.time(metricRegistry, RELEASE_TIMER, time, logContext);
      MetricUtil.meter(metricRegistry, RELEASE_METER, 1);
    }
  }

  @Override
  public TLlamaAMGetNodesResponse GetNodes(TLlamaAMGetNodesRequest request)
      throws TException {
    TLlamaAMGetNodesResponse response = null;
    long time = System.currentTimeMillis();
    try {
      response = service.GetNodes(request);
      return response;
    } finally {
      time = System.currentTimeMillis() - time;
      Object logContext = (response != null) ?
        new LogContext(response.getStatus(), request.getAm_handle()) : null;
      MetricUtil.time(metricRegistry, GET_NODES_TIMER, time, logContext);
    }
  }

}

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

import com.cloudera.llama.am.api.Expansion;
import com.cloudera.llama.am.api.LlamaAM;
import com.cloudera.llama.am.api.Reservation;
import com.cloudera.llama.server.ClientNotificationService;
import com.cloudera.llama.server.NodeMapper;
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
import com.cloudera.llama.thrift.TNetworkAddress;
import com.cloudera.llama.util.ErrorCode;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.util.UUID;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class LlamaAMServiceImpl implements LlamaAMService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(
      LlamaAMServiceImpl.class);

  private final LlamaAM llamaAM;
  private final NodeMapper nodeMapper;
  private final ClientNotificationService clientNotificationService;
  private final AtomicReference<AllocationConfiguration> allocConf;

  @SuppressWarnings("unchecked")
  public LlamaAMServiceImpl(LlamaAM llamaAM, NodeMapper nodeMapper,
      ClientNotificationService clientNotificationService,
      AtomicReference<AllocationConfiguration> allocConf) {
    this.llamaAM = llamaAM;
    this.nodeMapper = nodeMapper;
    this.clientNotificationService = clientNotificationService;
    this.allocConf = allocConf;
    llamaAM.addListener(clientNotificationService);
  }

  @Override
  public TLlamaAMRegisterResponse Register(TLlamaAMRegisterRequest request)
      throws TException {
    TLlamaAMRegisterResponse response = new TLlamaAMRegisterResponse();
    try {
      UUID clientId = TypeUtils.toUUID(request.getClient_id());
      TNetworkAddress tAddress = request.getNotification_callback_service();
      UUID handle = clientNotificationService.register(clientId,
          tAddress.getHostname(), tAddress.getPort());
      response.setStatus(TypeUtils.OK);
      response.setAm_handle(TypeUtils.toTUniqueId(handle));
    } catch (Throwable ex) {
      LOG.warn("Register() error: {}", ex.toString(), ex);
      response.setStatus(TypeUtils.createError(ex));
    }
    return response;
  }

  @Override
  public TLlamaAMUnregisterResponse Unregister(
      TLlamaAMUnregisterRequest request) throws TException {
    TLlamaAMUnregisterResponse response = new TLlamaAMUnregisterResponse();
    try {
      UUID handle = TypeUtils.toUUID(request.getAm_handle());
      if (!clientNotificationService.unregister(handle)) {
        LOG.warn("Unregister() unknown handle '{}'", handle);
      }
      response.setStatus(TypeUtils.OK);
    } catch (Exception ex) {
      LOG.warn("Unregister() internal error: {}", ex.toString(), ex);
      response.setStatus(TypeUtils.createError(ex));
    }
    return response;
  }

  @Override
  public TLlamaAMReservationResponse Reserve(TLlamaAMReservationRequest request)
      throws TException {
    TLlamaAMReservationResponse response = new TLlamaAMReservationResponse();
    try {
      UUID handle = TypeUtils.toUUID(request.getAm_handle());
      clientNotificationService.validateHandle(handle);

      String queue = assignToQueue(request);
      checkAccess(request.getUser(), queue, request.getQueue());

      Reservation reservation = TypeUtils.toReservation(request, nodeMapper, queue);
      UUID reservationId = llamaAM.reserve(reservation);
      response.setReservation_id(TypeUtils.toTUniqueId(reservationId));
      response.setStatus(TypeUtils.OK);
    } catch (Throwable ex) {
      LOG.warn("Reserve() error: {}", ex.toString(), ex);
      response.setStatus(TypeUtils.createError(ex));
    }
    return response;
  }
  
  /**
   * Assign reservation to a queue based on the placement policy specified
   * in the alloc conf
   */
  // Visible for testing
  String assignToQueue(TLlamaAMReservationRequest request)
      throws LlamaException {
    // Default means no queue requested
    String requestedQueue = (request.isSetQueue()) ? request.getQueue()
        : YarnConfiguration.DEFAULT_QUEUE_NAME;
    if (requestedQueue == null) {
      requestedQueue = YarnConfiguration.DEFAULT_QUEUE_NAME;
    }
    String user = request.getUser();
    String queue;
    try {
      queue = allocConf.get().getPlacementPolicy()
          .assignAppToQueue(requestedQueue, user);
    } catch (IOException ex) {
      throw new LlamaException(ex, ErrorCode.INTERNAL_ERROR);
    }
    if (queue == null) {
      throw new LlamaException(
          ErrorCode.RESERVATION_USER_TO_QUEUE_MAPPING_NOT_FOUND, requestedQueue,
          user);
    }
    LOG.debug("Reservation from user " + user + " with requested queue " +
        requestedQueue + " resolved to queue " + queue);
    
    return queue;
  }

  // Visible for testing
  void checkAccess(String user, String queue, String requestedQueue)
      throws LlamaException {
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.createProxyUser(user,
          UserGroupInformation.getCurrentUser());
    } catch (IOException ex) {
      throw new LlamaException(ex, ErrorCode.INTERNAL_ERROR);
    }
    if (!allocConf.get().hasAccess(queue, QueueACL.SUBMIT_APPLICATIONS, ugi)) {
      throw new LlamaException(ErrorCode.RESERVATION_USER_NOT_ALLOWED_IN_QUEUE,
          user, requestedQueue, queue);
    }
  }

  @Override
  public TLlamaAMReservationExpansionResponse Expand(
      TLlamaAMReservationExpansionRequest request) throws TException {
    TLlamaAMReservationExpansionResponse response =
        new TLlamaAMReservationExpansionResponse();
    try {
      UUID handle = TypeUtils.toUUID(request.getAm_handle());
      clientNotificationService.validateHandle(handle);
      Expansion expansion = TypeUtils.toExpansion(request, nodeMapper);
      UUID reservationId = llamaAM.expand(expansion);
      response.setReservation_id(TypeUtils.toTUniqueId(reservationId));
      response.setStatus(TypeUtils.OK);
    } catch (Throwable ex) {
      LOG.warn("Expand() error: {}", ex.toString(), ex);
      response.setStatus(TypeUtils.createError(ex));
    }
    return response;
  }

  @Override
  public TLlamaAMReleaseResponse Release(TLlamaAMReleaseRequest request)
      throws TException {
    TLlamaAMReleaseResponse response = new TLlamaAMReleaseResponse();
    try {
      UUID handle = TypeUtils.toUUID(request.getAm_handle());
      clientNotificationService.validateHandle(handle);
      UUID reservationId = TypeUtils.toUUID(request.getReservation_id());
      llamaAM.releaseReservation(handle, reservationId, false);
      response.setStatus(TypeUtils.OK);
    } catch (Throwable ex) {
      LOG.warn("Release() error: {}", ex.toString(), ex);
      response.setStatus(TypeUtils.createError(ex));
    }
    return response;
  }

  @Override
  public TLlamaAMGetNodesResponse GetNodes(TLlamaAMGetNodesRequest request)
      throws TException {
    TLlamaAMGetNodesResponse response = new TLlamaAMGetNodesResponse();
    try {
      UUID handle = TypeUtils.toUUID(request.getAm_handle());
      clientNotificationService.validateHandle(handle);
      List<String> nodes = nodeMapper.getDataNodes(llamaAM.getNodes());
      response.setNodes(nodes);
      response.setStatus(TypeUtils.OK);
    } catch (Throwable ex) {
      LOG.warn("GetNodes() error: {}", ex.toString(), ex);
      response.setStatus(TypeUtils.createError(ex));
    }
    return response;
  }

}

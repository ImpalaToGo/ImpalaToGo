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
package com.cloudera.llama.nm;

import com.cloudera.llama.server.ClientNotificationService;
import com.cloudera.llama.server.TypeUtils;
import com.cloudera.llama.thrift.LlamaNMService;
import com.cloudera.llama.thrift.TLlamaNMRegisterResponse;
import com.cloudera.llama.thrift.TLlamaNMRegisterRequest;
import com.cloudera.llama.thrift.TLlamaNMUnregisterRequest;
import com.cloudera.llama.thrift.TLlamaNMUnregisterResponse;
import com.cloudera.llama.thrift.TNetworkAddress;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.llama.util.UUID;

public class LlamaNMServiceImpl implements LlamaNMService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(
      LlamaNMServiceImpl.class);

  private final ClientNotificationService clientNotificationService;

  public LlamaNMServiceImpl(ClientNotificationService clientNotificationService) {
    this.clientNotificationService = clientNotificationService;
  }

  @Override
  public TLlamaNMRegisterResponse Register(TLlamaNMRegisterRequest request)
      throws TException {
    TLlamaNMRegisterResponse response = new TLlamaNMRegisterResponse();
    try {
      UUID clientId = TypeUtils.toUUID(request.getClient_id());
      TNetworkAddress tAddress = request.getNotification_callback_service();
      UUID handle = clientNotificationService.register(clientId,
          tAddress.getHostname(), tAddress.getPort());
      response.setStatus(TypeUtils.OK);
      response.setNm_handle(TypeUtils.toTUniqueId(handle));
    } catch (Throwable ex) {
      LOG.warn("Register() error: {}", ex.toString(), ex);
      response.setStatus(TypeUtils.createError(ex));
    }
    return response;
  }

  @Override
  public TLlamaNMUnregisterResponse Unregister(
      TLlamaNMUnregisterRequest request) throws TException {
    TLlamaNMUnregisterResponse response = new TLlamaNMUnregisterResponse();
    try {
      UUID handle = TypeUtils.toUUID(request.getNm_handle());
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

}

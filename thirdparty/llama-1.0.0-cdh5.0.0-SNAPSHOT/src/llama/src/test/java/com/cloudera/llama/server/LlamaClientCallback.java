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
import com.cloudera.llama.thrift.TUniqueId;
import com.cloudera.llama.util.UUID;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class LlamaClientCallback extends
    ThriftServer<LlamaNotificationService.Processor, TProcessor> {
  public static final String PORT_KEY = "llama.client.callback.port";

  private static final Logger LOG =
      LoggerFactory.getLogger(LlamaClientCallback.class);

  public static class ClientCallbackServerConfiguration
      extends ServerConfiguration {

    public ClientCallbackServerConfiguration() {
      super("cc");
    }

    @Override
    public int getThriftDefaultPort() {
      return getConf().getInt(PORT_KEY, 0);
    }

    @Override
    public int getHttpDefaultPort() {
      return 0;
    }
  }

  private static Map<UUID, CountDownLatch> latches =
      new ConcurrentHashMap<UUID, CountDownLatch>();

  public static CountDownLatch getReservationLatch(UUID reservation) {
    CountDownLatch latch = new CountDownLatch(1);
    latches.put(reservation, latch);
    return latch;
  }

  private static void notifyStatusChange(UUID reservation) {
    CountDownLatch latch = latches.remove(reservation);
    if (latch != null) {
      latch.countDown();
    }
  }

  public LlamaClientCallback() {
    super("LlamaClientCallback", ClientCallbackServerConfiguration.class);
  }

  public static class LNServiceImpl implements LlamaNotificationService.Iface {

    @Override
    public TLlamaAMNotificationResponse AMNotification(
        TLlamaAMNotificationRequest request) throws TException {
      LOG.info(request.toString());
      if (request.isSetAllocated_reservation_ids()) {
        for (TUniqueId r : request.getAllocated_reservation_ids()) {
          notifyStatusChange(TypeUtils.toUUID(r));
        }
      }
      if (request.isSetPreempted_reservation_ids()) {
        for (TUniqueId r : request.getPreempted_reservation_ids()) {
          notifyStatusChange(TypeUtils.toUUID(r));
        }
      }
      if (request.isSetRejected_reservation_ids()) {
        for (TUniqueId r : request.getRejected_reservation_ids()) {
          notifyStatusChange(TypeUtils.toUUID(r));
        }
      }
      if (request.isSetLost_reservation_ids()) {
        for (TUniqueId r : request.getLost_reservation_ids()) {
          notifyStatusChange(TypeUtils.toUUID(r));
        }
      }
      if (request.isSetAdmin_released_reservation_ids()) {
        for (TUniqueId r : request.getAdmin_released_reservation_ids()) {
          notifyStatusChange(TypeUtils.toUUID(r));
        }
      }
      return new TLlamaAMNotificationResponse().setStatus(TypeUtils.OK);
    }

    @Override
    public TLlamaNMNotificationResponse NMNotification(
        TLlamaNMNotificationRequest request) throws TException {
      LOG.info(request.toString());
      return new TLlamaNMNotificationResponse().setStatus(TypeUtils.OK);
    }
  }

  @Override
  protected LlamaNotificationService.Processor createServiceProcessor() {
    Class<? extends LlamaNotificationService.Iface> klass = getConf().getClass(
        "cc.handler.class", LNServiceImpl.class,
        LlamaNotificationService.Iface.class);
    LlamaNotificationService.Iface handler =
        ReflectionUtils.newInstance(klass, getConf());
    return new LlamaNotificationService.Processor<LlamaNotificationService
        .Iface>(handler);
  }

  @Override
  protected void startService() {
  }

  @Override
  protected void stopService() {
  }

}

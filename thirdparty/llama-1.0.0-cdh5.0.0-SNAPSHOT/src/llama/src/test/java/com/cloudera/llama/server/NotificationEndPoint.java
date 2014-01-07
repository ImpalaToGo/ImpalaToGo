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
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NotificationEndPoint extends
    ThriftServer<LlamaNotificationService.Processor, TProcessor> {
  public List<TLlamaAMNotificationRequest> notifications;
  public volatile long delayResponse;

  public static class ClientServerConfiguration
      extends ServerConfiguration {

    public ClientServerConfiguration() {
      super("client");
    }

    @Override
    public int getThriftDefaultPort() {
      return 0;
    }

    @Override
    public int getHttpDefaultPort() {
      return 0;
    }
  }

  public NotificationEndPoint() {
    super("NotificationEndPoint", ClientServerConfiguration.class);
  }

  @Override
  protected LlamaNotificationService.Processor createServiceProcessor() {
    LlamaNotificationService.Iface handler =
        new LlamaNotificationService.Iface() {
          @Override
          public TLlamaAMNotificationResponse AMNotification(
              TLlamaAMNotificationRequest request) throws TException {
            if (delayResponse == 0) {
              notifications.add(request);
            } else {
              try {
                Thread.sleep(delayResponse);
              } catch (InterruptedException ex) {
                //NOP
              }
            }
            return new TLlamaAMNotificationResponse().setStatus(TypeUtils.OK);
          }

          @Override
          public TLlamaNMNotificationResponse NMNotification(
              TLlamaNMNotificationRequest request) throws TException {
            throw new UnsupportedOperationException();
          }
        };
    return new LlamaNotificationService.Processor<LlamaNotificationService
        .Iface>(handler);
  }

  @Override
  protected void startService() {
    notifications = Collections.synchronizedList(
        new ArrayList<TLlamaAMNotificationRequest>());
    delayResponse = 0;
  }

  @Override
  protected void stopService() {
    notifications = null;
  }

}

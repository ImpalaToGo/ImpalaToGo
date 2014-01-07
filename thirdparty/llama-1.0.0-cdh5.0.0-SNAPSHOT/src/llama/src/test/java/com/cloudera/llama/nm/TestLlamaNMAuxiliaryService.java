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

import com.cloudera.llama.server.TypeUtils;
import com.cloudera.llama.thrift.LlamaNMService;
import com.cloudera.llama.thrift.TLlamaNMUnregisterRequest;
import com.cloudera.llama.thrift.TLlamaNMUnregisterResponse;
import com.cloudera.llama.thrift.TLlamaNMRegisterRequest;
import com.cloudera.llama.thrift.TLlamaNMRegisterResponse;
import com.cloudera.llama.thrift.TLlamaServiceVersion;
import com.cloudera.llama.thrift.TNetworkAddress;
import com.cloudera.llama.thrift.TStatusCode;
import com.cloudera.llama.thrift.TUniqueId;
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;

public class TestLlamaNMAuxiliaryService {

  public static class MyLlamaNMAuxiliaryService
      extends LlamaNMAuxiliaryService {
    static String host;
    static int port;
    
    @Override
    public void start() {
      super.start();
      LlamaNMServer server = getNMServer();
      host = server.getAddressHost();
      port = server.getAddressPort();
    }
  }

  protected NMServerConfiguration sConf = new NMServerConfiguration();
  private static MiniYARNCluster miniYarn;

  protected void injectLlamaNMConfiguration(Configuration conf)
      throws Exception {
    conf.set(sConf.getPropertyName(NMServerConfiguration.SERVER_ADDRESS_KEY),
        "localhost:0");
  }

  private Configuration createMiniYarnConfig() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.set("yarn.nodemanager.aux-services", "llama_nm_plugin");
    conf.setClass("yarn.nodemanager.aux-services.llama_nm_plugin.class",
        MyLlamaNMAuxiliaryService.class, AuxiliaryService.class);

    injectLlamaNMConfiguration(conf);
    return conf;
  }

  private void startYarn(Configuration conf) throws Exception {
    miniYarn = new MiniYARNCluster("llama.nm.plugin", 1, 1, 1);
    miniYarn.init(conf);
    miniYarn.start();
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }

  private void stopYarn() {
    if (miniYarn != null) {
      miniYarn.stop();
      miniYarn = null;
    }
  }

  protected LlamaNMService.Client createClient()
      throws Exception {
    TTransport transport = new TSocket(MyLlamaNMAuxiliaryService.host,
        MyLlamaNMAuxiliaryService.port);
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    return new LlamaNMService.Client(protocol);
  }

  protected Subject getClientSubject() throws Exception {
    return new Subject();
  }

  @Test
  public void testStartRegisterUnregisterStop() throws Exception {
    try {
      startYarn(createMiniYarnConfig());

      Subject.doAs(getClientSubject(), new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          LlamaNMService.Client client = createClient();


          TLlamaNMRegisterRequest trReq = new TLlamaNMRegisterRequest();
          trReq.setVersion(TLlamaServiceVersion.V1);
          trReq.setClient_id(TypeUtils.toTUniqueId(UUID.randomUUID()));
          TNetworkAddress tAddress = new TNetworkAddress();
          tAddress.setHostname("localhost");
          tAddress.setPort(0);
          trReq.setNotification_callback_service(tAddress);

          //register
          TLlamaNMRegisterResponse trRes = client.Register(trReq);
          Assert.assertEquals(TStatusCode.OK, trRes.getStatus().getStatus_code());
          Assert.assertNotNull(trRes.getNm_handle());
          Assert.assertNotNull(TypeUtils.toUUID(trRes.getNm_handle()));
          TUniqueId handle = trRes.getNm_handle();

          //valid re-register
          trRes = client.Register(trReq);
          Assert.assertEquals(TStatusCode.OK, trRes.getStatus().getStatus_code());
          Assert.assertNotNull(trRes.getNm_handle());
          Assert.assertNotNull(TypeUtils.toUUID(trRes.getNm_handle()));

          //invalid re-register different address
          tAddress.setPort(1);
          trRes = client.Register(trReq);
          Assert.assertEquals(TStatusCode.REQUEST_ERROR, trRes.getStatus().
              getStatus_code());

          TLlamaNMUnregisterRequest turReq = new TLlamaNMUnregisterRequest();
          turReq.setVersion(TLlamaServiceVersion.V1);
          turReq.setNm_handle(handle);

          //valid unRegister
          TLlamaNMUnregisterResponse turRes = client.Unregister(turReq);
          Assert.assertEquals(TStatusCode.OK, turRes.getStatus().getStatus_code());

          //valid re-unRegister
          turRes = client.Unregister(turReq);
          Assert.assertEquals(TStatusCode.OK, turRes.getStatus().getStatus_code());

          return null;
        }
      });

    } finally {
      stopYarn();
    }
  }

}

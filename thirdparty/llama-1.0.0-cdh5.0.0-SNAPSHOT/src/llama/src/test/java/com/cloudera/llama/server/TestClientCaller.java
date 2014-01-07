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
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestClientCaller {

  static boolean createClient;

  public static class CSServerConfiguration
      extends ServerConfiguration {

    public CSServerConfiguration() {
      super("cs");
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

  public static class MyClientCaller extends ClientCaller {

    public MyClientCaller(UUID clientId, UUID handle,
        String host, int port) {
      super(new CSServerConfiguration(), clientId, handle, host, port, null);
    }

    @Override
    LlamaNotificationService.Iface createClient() throws Exception {
      createClient = true;
      return Mockito.mock(LlamaNotificationService.Iface.class);
    }
  }

  @Test
  public void testClientCallerOK() throws Exception {
    final UUID cId = UUID.randomUUID();
    final UUID handle = UUID.randomUUID();
    ClientCaller cc = new MyClientCaller(cId, handle, "h", 0);
    Assert.assertEquals(cId, cc.getClientId());

    ClientCaller.Callable<Boolean> callable =
        new ClientCaller.Callable<Boolean>() {
          @Override
          public Boolean call() throws ClientException {
            Assert.assertEquals(cId, getClientId());
            Assert.assertEquals(handle, getHandle());
            Assert.assertNotNull(getClient());
            return Boolean.TRUE;
          }
        };
    createClient = false;
    Assert.assertTrue(cc.execute(callable));
    Assert.assertTrue(createClient);
    createClient = false;
    Assert.assertTrue(cc.execute(callable));
    Assert.assertFalse(createClient);
  }

  @Test(expected = ClientException.class)
  public void testClientCallerFail() throws Exception {
    final UUID cId = UUID.randomUUID();
    final UUID handle = UUID.randomUUID();
    ClientCaller cc = new MyClientCaller(cId, handle, "h", 0);
    Assert.assertEquals(cId, cc.getClientId());

    ClientCaller.Callable<Void> callable =
        new ClientCaller.Callable<Void>() {
          @Override
          public Void call() throws ClientException {
            throw new ClientException(new Exception());
          }
        };
    try {
      createClient = false;
      cc.execute(callable);
    } finally {
      Assert.assertTrue(createClient);
      createClient = false;
      callable = new ClientCaller.Callable<Void>() {
        @Override
        public Void call() throws ClientException {
          return null;
        }
      };
      cc.execute(callable);
      Assert.assertTrue(createClient);
    }
  }

}

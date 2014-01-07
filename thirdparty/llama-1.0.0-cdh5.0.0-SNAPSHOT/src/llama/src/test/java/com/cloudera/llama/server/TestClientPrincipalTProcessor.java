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

import junit.framework.Assert;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;
import org.mockito.Mockito;

import javax.security.sasl.SaslServer;

public class TestClientPrincipalTProcessor implements TProcessor {
  private boolean invoked;
  private String user;

  //doing this trick because ghet getSaslServer() method is defined in
  //the TSaslTransport class which is package private and Mockito fails
  //to mock that method because of that.
  public static class MyTSaslServerTransport extends TSaslServerTransport {
    public MyTSaslServerTransport(TTransport transport) {
      super(transport);
    }

    @Override
    public SaslServer getSaslServer() {
      return null;
    }
  }

  @Override
  public boolean process(TProtocol tProtocol, TProtocol tProtocol2)
      throws TException {
    invoked = true;
    Assert.assertEquals(user, ClientPrincipalTProcessor.getPrincipal());
    return true;
  }

  @Test
  public void testSetGetPrincipal() throws Exception {
    invoked = false;
    user = "foo";
    SaslServer saslServer = Mockito.mock(SaslServer.class);
    Mockito.when(saslServer.getAuthorizationID()).thenReturn("foo");
    MyTSaslServerTransport tst = Mockito.mock(MyTSaslServerTransport.class);
    Mockito.when(tst.getSaslServer()).thenReturn(saslServer);
    Mockito.when(tst.getSaslServer()).thenReturn(saslServer);
    TProtocol p = Mockito.mock(TProtocol.class);
    Mockito.when(p.getTransport()).thenReturn(tst);

    TProcessor cpTp = new ClientPrincipalTProcessor(this);

    Assert.assertNull(ClientPrincipalTProcessor.getPrincipal());
    cpTp.process(p, p);
    Assert.assertTrue(invoked);
    Assert.assertNull(ClientPrincipalTProcessor.getPrincipal());
  }

  @Test
  public void testUnsetGetPrincipal() throws Exception {
    invoked = false;
    user = null;
    TProtocol p = Mockito.mock(TProtocol.class);

    Assert.assertNull(ClientPrincipalTProcessor.getPrincipal());
    this.process(p, p);
    Assert.assertTrue(invoked);
    Assert.assertNull(ClientPrincipalTProcessor.getPrincipal());
  }

}

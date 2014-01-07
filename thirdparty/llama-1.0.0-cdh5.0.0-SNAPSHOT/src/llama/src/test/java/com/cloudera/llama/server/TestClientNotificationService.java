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

import com.cloudera.llama.am.AMServerConfiguration;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.util.UUID;
import junit.framework.Assert;
import org.junit.Test;

public class TestClientNotificationService {

  public static class MyListener implements
      ClientNotificationService.Listener {

    private boolean registered;
    private boolean unregistered;

    @Override
    public void onRegister(ClientInfo clientInfo) {
      registered = true;
    }

    @Override
    public void onUnregister(ClientInfo clientInfo) {
      unregistered = true;
    }

  }

  @Test
  public void testRegisterNewClientIdNewCallback() throws Exception {
    MyListener ul = new MyListener();
    ClientNotificationService cns = new ClientNotificationService(
        new AMServerConfiguration(), null, null);
    cns.addListener(ul);
    cns.start();
    try {
      UUID c1 = UUID.randomUUID();
      UUID handle = cns.register(c1, "h", 0);
      Assert.assertTrue(ul.registered);
      Assert.assertFalse(ul.unregistered);
      Assert.assertNotNull(handle);
      ul.registered = false;
      Assert.assertTrue(cns.unregister(handle));
      Assert.assertFalse(ul.registered);
      Assert.assertTrue(ul.unregistered);
    } finally {
      cns.stop();
    }
  }

  @Test
  public void testRegisterNewClientIdExistingCallback() throws Exception {
    MyListener ul = new MyListener();
    ClientNotificationService cns = new ClientNotificationService(
        new AMServerConfiguration(), null, null);
    cns.addListener(ul);
    cns.start();
    try {
      UUID c1 = UUID.randomUUID();
      UUID handle1 = cns.register(c1, "h", 0);
      Assert.assertNotNull(handle1);
      UUID c2 = UUID.randomUUID();
      UUID handle2 = cns.register(c2, "h", 0);
      Assert.assertNotSame(handle1 ,handle2);
      Assert.assertTrue(ul.unregistered);
      ul.unregistered = false;
      Assert.assertTrue(cns.unregister(handle2));
      Assert.assertTrue(ul.unregistered);
    } finally {
      cns.stop();
    }
  }

  @Test
  public void testRegisterExistingClientIdExistingCallbackSameHandle()
      throws Exception {
    MyListener ul = new MyListener();
    ClientNotificationService cns = new ClientNotificationService(
        new AMServerConfiguration(), null, null);
    cns.addListener(ul);
    cns.start();
    try {
      UUID c1 = UUID.randomUUID();
      UUID handle1 = cns.register(c1, "h", 0);
      Assert.assertNotNull(handle1);
      UUID handle2 = cns.register(c1, "h", 0);
      Assert.assertEquals(handle1, handle2);
      Assert.assertFalse(ul.unregistered);
      Assert.assertTrue(cns.unregister(handle2));
      Assert.assertTrue(ul.unregistered);
    } finally {
      cns.stop();
    }
  }

  @Test(expected = LlamaException.class)
  public void testRegisterExistingClientIdExistingCallbackDifferentHandle()
      throws Exception {
    MyListener ul = new MyListener();
    ClientNotificationService cns = new ClientNotificationService(
        new AMServerConfiguration(), null, null);
    cns.addListener(ul);
    cns.start();
    try {
      UUID c1 = UUID.randomUUID();
      UUID handle1 = cns.register(c1, "h1", 0);
      Assert.assertNotNull(handle1);
      UUID c2 = UUID.randomUUID();
      UUID handle2 = cns.register(c2, "h2", 0);
      Assert.assertNotNull(handle2);
      Assert.assertNotSame(handle1, handle2);
      cns.register(c1, "h2", 0);
    } finally {
      Assert.assertFalse(ul.unregistered);
      cns.stop();
    }
  }

  @Test(expected = LlamaException.class)
  public void testRegisterExistingClientIdNonExistingCallback()
      throws Exception {
    MyListener ul = new MyListener();
    ClientNotificationService cns = new ClientNotificationService(
        new AMServerConfiguration(), null, null);
    cns.addListener(ul);
    cns.start();
    try {
      UUID c1 = UUID.randomUUID();
      UUID handle1 = cns.register(c1, "h1", 0);
      Assert.assertNotNull(handle1);
      cns.register(c1, "h2", 0);
    } finally {
      Assert.assertFalse(ul.unregistered);
      cns.stop();
    }
  }

}

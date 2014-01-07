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

import com.cloudera.llama.am.HostnameOnlyNodeMapper;
import com.cloudera.llama.am.api.PlacedReservation;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.am.impl.LlamaAMEventImpl;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.llama.util.UUID;

public class TestClientNotifier {

  public static class NSServerConfiguration
      extends ServerConfiguration {

    public NSServerConfiguration(Configuration conf) {
      super("ns", conf);
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

  private ServerConfiguration nsConf = new NSServerConfiguration(
      new Configuration(false));

  private NotificationEndPoint notificationServer;

  @Before
  public void startNotificationServer() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(ServerConfiguration.CONFIG_DIR_KEY,
        TestAbstractMain.createTestDir());
    conf.set(nsConf.getPropertyName(
        ServerConfiguration.SERVER_ADDRESS_KEY), "localhost:0");
    conf.set(nsConf.getPropertyName(
        ServerConfiguration.HTTP_ADDRESS_KEY), "localhost:0");
    notificationServer = new NotificationEndPoint();
    notificationServer.setConf(conf);
    notificationServer.start();
  }

  @After
  public void stopNotificationServer() throws Exception {
    if (notificationServer != null) {
      notificationServer.stop();
    }
  }

  public static class MyClientRegistry implements
      ClientNotifier.ClientRegistry {

    private ClientCaller clientCaller;
    volatile int clientCallerCalls = 0;
    boolean maxFailures;

    public MyClientRegistry(ServerConfiguration conf, UUID clientId,
        UUID handle, String host, int port) {
      clientCaller = new ClientCaller(conf, clientId, handle, host, port, null);
    }

    @Override
    public ClientCaller getClientCaller(UUID handle) {
      clientCallerCalls++;
      return clientCaller;
    }

    @Override
    public void onMaxFailures(UUID handle) {
      maxFailures = true;
    }

  }

  @Test
  public void testHeartbeats() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setInt(nsConf.getPropertyName(
        ServerConfiguration.CLIENT_NOTIFIER_HEARTBEAT_KEY), 300);
    ServerConfiguration sConf = new NSServerConfiguration(conf);
    UUID clientId = UUID.randomUUID();
    UUID handle = UUID.randomUUID();
    MyClientRegistry cr = new MyClientRegistry(sConf, clientId, handle,
        notificationServer.getAddressHost(),
        notificationServer.getAddressPort());
    ClientNotifier cn = new ClientNotifier(sConf, new HostnameOnlyNodeMapper(),
        cr, null);
    try {
      cn.start();
      cn.registerClientForHeartbeats(handle);
      Assert.assertEquals(0, notificationServer.notifications.size());
      Thread.sleep(350); //adding 50ms extra
      Assert.assertEquals(1, notificationServer.notifications.size());
      Assert.assertTrue(notificationServer.notifications.get(0).isHeartbeat());
      notificationServer.notifications.clear();
      Thread.sleep(350); //adding 50ms extra
      Assert.assertEquals(1, notificationServer.notifications.size());
      Assert.assertTrue(notificationServer.notifications.get(0).isHeartbeat());
    } finally {
      cn.stop();
    }
  }

  @Test
  public void testNotificationAndHeartbeatReset() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setInt(nsConf.getPropertyName(
        ServerConfiguration.CLIENT_NOTIFIER_HEARTBEAT_KEY), 300);
    ServerConfiguration sConf = new NSServerConfiguration(conf);
    UUID clientId = UUID.randomUUID();
    UUID handle = UUID.randomUUID();
    MyClientRegistry cr = new MyClientRegistry(sConf, clientId, handle,
        notificationServer.getAddressHost(),
        notificationServer.getAddressPort());
    ClientNotifier cn = new ClientNotifier(sConf, new HostnameOnlyNodeMapper(),
        cr, null);
    try {
      cn.start();
      cn.registerClientForHeartbeats(handle);
      LlamaAMEventImpl event = new LlamaAMEventImpl();
      event.addReservation(TestUtils.createPlacedReservation(
          TestUtils.createReservation(true),
          PlacedReservation.Status.ALLOCATED));
      Thread.sleep(100);
      cn.onEvent(event);
      Assert.assertEquals(0, notificationServer.notifications.size());
      Thread.sleep(180);
      Assert.assertEquals(1, notificationServer.notifications.size());
      Assert.assertFalse(notificationServer.notifications.get(0).isHeartbeat());
      notificationServer.notifications.clear();
      Thread.sleep(170); //adding 50ms extra
      Assert.assertEquals(1, notificationServer.notifications.size());
      Assert.assertTrue(notificationServer.notifications.get(0).isHeartbeat());
    } finally {
      cn.stop();
    }
  }

  @Test
  public void testRetriesAndMaxFailures() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setInt(nsConf.getPropertyName(
        ServerConfiguration.CLIENT_NOTIFIER_HEARTBEAT_KEY), 10);
    conf.setInt(nsConf.getPropertyName(
        ServerConfiguration.CLIENT_NOTIFIER_RETRY_INTERVAL_KEY), 10);
    conf.setInt(nsConf.getPropertyName(
        ServerConfiguration.CLIENT_NOTIFIER_MAX_RETRIES_KEY), 2);
    conf.setInt(nsConf.getPropertyName(
        ServerConfiguration.TRANSPORT_TIMEOUT_KEY), 10);
    ServerConfiguration sConf = new NSServerConfiguration(conf);
    UUID clientId = UUID.randomUUID();
    UUID handle = UUID.randomUUID();
    MyClientRegistry cr = new MyClientRegistry(sConf, clientId, handle,
        notificationServer.getAddressHost(), 0);
    ClientNotifier cn = new ClientNotifier(sConf, new HostnameOnlyNodeMapper(),
        cr, null);
    try {
      cn.start();
      cn.registerClientForHeartbeats(handle);
      Thread.sleep(100); //adding 70ms extra
      Assert.assertEquals(3, cr.clientCallerCalls);
      Assert.assertTrue(cr.maxFailures);
    } finally {
      cn.stop();
    }
  }

  @Test
  public void testRetryWithRecovery() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setInt(nsConf.getPropertyName(
        ServerConfiguration.CLIENT_NOTIFIER_HEARTBEAT_KEY), 10000);
    conf.setInt(nsConf.getPropertyName(
        ServerConfiguration.CLIENT_NOTIFIER_RETRY_INTERVAL_KEY), 200);
    conf.setInt(nsConf.getPropertyName(
        ServerConfiguration.CLIENT_NOTIFIER_MAX_RETRIES_KEY), 2);
    conf.setInt(nsConf.getPropertyName(
        ServerConfiguration.TRANSPORT_TIMEOUT_KEY), 50);
    ServerConfiguration sConf = new NSServerConfiguration(conf);
    UUID clientId = UUID.randomUUID();
    UUID handle = UUID.randomUUID();
    MyClientRegistry cr = new MyClientRegistry(sConf, clientId, handle,
        notificationServer.getAddressHost(),
        notificationServer.getAddressPort());
    ClientNotifier cn = new ClientNotifier(sConf, new HostnameOnlyNodeMapper(),
        cr, null);
    notificationServer.delayResponse = 100;
    try {
      cn.start();
      cn.registerClientForHeartbeats(handle);

      LlamaAMEventImpl event = new LlamaAMEventImpl();
      event.addReservation(TestUtils.createPlacedReservation(
          TestUtils.createReservation(true),
          PlacedReservation.Status.ALLOCATED));
      cn.onEvent(event);
      Thread.sleep(100); //adding 50ms extra
      Assert.assertEquals(1, cr.clientCallerCalls);
      Assert.assertFalse(cr.maxFailures);
      cr.clientCallerCalls = 0;
      notificationServer.delayResponse = 0;
      notificationServer.notifications.clear();
      Thread.sleep(250); //adding 50ms extra
      Assert.assertEquals(1, cr.clientCallerCalls);
      Assert.assertEquals(1, notificationServer.notifications.size());
      Assert.assertFalse(notificationServer.notifications.get(0).isHeartbeat());
      Assert.assertFalse(cr.maxFailures);
    } finally {
      cn.stop();
    }
  }

}

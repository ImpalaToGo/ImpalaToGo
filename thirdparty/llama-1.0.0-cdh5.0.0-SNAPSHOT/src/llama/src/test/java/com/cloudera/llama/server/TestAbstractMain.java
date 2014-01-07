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

import com.cloudera.llama.am.api.LlamaAM;
import com.cloudera.llama.am.mock.MockRMConnector;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.Writer;
import java.util.concurrent.CountDownLatch;

import com.cloudera.llama.util.UUID;

import javax.security.auth.Subject;

public class TestAbstractMain {
  public static final String LLAMA_BUILD_DIR = "test.llama.build.dir";

  public static class XServerConfiguration
      extends ServerConfiguration {

    public XServerConfiguration() {
      super("x", new Configuration(false));
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

  private ServerConfiguration sConf = new XServerConfiguration();

  public static String createTestDir() {
    File dir = new File(System.getProperty(LLAMA_BUILD_DIR, "target"));
    //Using JDK UUID because the Llama UUID string format has ':' and this
    // breaks things when used in ENV vars
    dir = new File(dir, java.util.UUID.randomUUID().toString()).getAbsoluteFile();
    dir.mkdirs();
    return dir.getAbsolutePath();
  }

  @Before
  public void beforeTest() {
    System.setProperty(AbstractMain.TEST_LLAMA_JVM_EXIT_SYS_PROP, "true");
    System.getProperties().remove(AbstractMain.CONF_DIR_SYS_PROP);
    System.getProperties().remove(AbstractMain.LOG_DIR_SYS_PROP);
  }

  @After
  public void afterTest() {
    System.getProperties().remove(AbstractMain.TEST_LLAMA_JVM_EXIT_SYS_PROP);
    System.getProperties().remove(AbstractMain.CONF_DIR_SYS_PROP);
    System.getProperties().remove(AbstractMain.LOG_DIR_SYS_PROP);
  }

  private void createMainConf(String confDir, Configuration conf)
      throws Exception {
    System.setProperty(AbstractMain.CONF_DIR_SYS_PROP, confDir);
    conf.setIfUnset(LlamaAM.RM_CONNECTOR_CLASS_KEY, MockRMConnector
        .class.getName());
    conf.set(sConf.getPropertyName(ServerConfiguration.SERVER_ADDRESS_KEY),
        "localhost:0");
    conf.set(sConf.getPropertyName(ServerConfiguration.HTTP_ADDRESS_KEY),
        "localhost:0");
    Writer writer = new FileWriter(new File(confDir, "llama-site.xml"));
    conf.writeXml(writer);
    writer.close();
  }

  public static class MyServer extends AbstractServer {

    public MyServer() {
      super("foo");
    }

    @Override
    protected Subject loginServerSubject() {
      return new Subject();
    }

    @Override
    protected void startService() {
    }

    @Override
    protected void stopService() {
    }

    @Override
    protected void startTransport(CountDownLatch latch) {
      latch.countDown();
    }

    @Override
    protected void stopTransport() {
    }

    @Override
    public String getAddressHost() {
      return "h";
    }

    @Override
    public int getAddressPort() {
      return 1;
    }
  }

  public static class MyMain extends AbstractMain {

    @Override
    protected Class<? extends AbstractServer> getServerClass() {
      return MyServer.class;
    }
  }

  @Test
  public void testMainOK1() throws Exception {
    String testDir = createTestDir();
    createMainConf(testDir, new Configuration(false));
    final AbstractMain main = new MyMain();
    main.shutdown();
    Assert.assertEquals(0, main.run(null));
    main.waitStopLatch();
  }

  @Test
  public void testMainOK2() throws Exception {
    String testDir = createTestDir();
    createMainConf(testDir, new Configuration(false));
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    InputStream is = cl.getResourceAsStream("log4j.properties");
    FileUtils.copyInputStreamToFile(is, new File(testDir,
        "llama-log4j.properties"));
    System.setProperty(AbstractMain.CONF_DIR_SYS_PROP, testDir);
    System.setProperty(AbstractMain.LOG_DIR_SYS_PROP, testDir);
    final AbstractMain main = new MyMain();
    main.shutdown();
    Assert.assertEquals(0, main.run(null));
    main.waitStopLatch();
  }

  @Test(expected = RuntimeException.class)
  public void testServiceError1() throws Exception {
    AbstractMain.Service.verifyRequiredSysProps();
  }

  @Test(expected = RuntimeException.class)
  public void testServiceError2() throws Exception {
    String testDir = createTestDir();
    System.setProperty(AbstractMain.CONF_DIR_SYS_PROP, testDir);
    AbstractMain.Service.verifyRequiredSysProps();
  }

  @Test(expected = RuntimeException.class)
  public void testServiceError3() throws Exception {
    String testDir = createTestDir();
    System.setProperty(AbstractMain.CONF_DIR_SYS_PROP,
        UUID.randomUUID().toString());
    System.setProperty(AbstractMain.LOG_DIR_SYS_PROP, testDir);
    AbstractMain.Service.verifyRequiredSysProps();
  }

  @Test
  public void testServiceOK1() throws Exception {
    String testDir = createTestDir();
    createMainConf(testDir, new Configuration(false));
    System.setProperty(AbstractMain.CONF_DIR_SYS_PROP, testDir);
    System.setProperty(AbstractMain.LOG_DIR_SYS_PROP, testDir);
    AbstractMain.Service.verifyRequiredSysProps();
  }

}

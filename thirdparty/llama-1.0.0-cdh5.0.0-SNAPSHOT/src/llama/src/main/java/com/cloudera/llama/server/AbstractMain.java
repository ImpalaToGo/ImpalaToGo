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

import com.cloudera.llama.util.VersionInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractMain {
  public static final String TEST_LLAMA_JVM_EXIT_SYS_PROP =
      "test.llama.disable.jvm.exit";

  public static final String SERVER_CLASS_KEY = "llama.server.class";

  public static final String CONF_DIR_SYS_PROP = "llama.server.conf.dir";
  public static final String LOG_DIR_SYS_PROP = "llama.server.log.dir";

  public static class Service {

    static void verifyRequiredSysProps() {
      verifySystemPropertyDir(CONF_DIR_SYS_PROP, true);
      verifySystemPropertyDir(LOG_DIR_SYS_PROP, false);
    }

    public static void run(Class<? extends AbstractMain> mainClass,
        String[] args) throws Exception {
      verifyRequiredSysProps();
      mainClass.newInstance().run(args);
    }

    private static String verifySystemPropertyDir(String name,
        boolean mustExist) {
      String dir = System.getProperty(name);
      if (dir == null) {
        throw new RuntimeException("Undefined Java System Property '" + name +
            "'");
      }
      if (mustExist && !new File(dir).exists()) {
        throw new RuntimeException("Directory '" + dir + "' does not exist");
      }
      return dir;
    }
  }

  private static final String LOG4J_PROPERTIES = "llama-log4j.properties";
  private static final String SITE_XML = "llama-site.xml";

  private static Logger LOG;

  protected static void run(Class<? extends AbstractMain> mainClass,
      String[] args) throws Exception {
    AbstractMain main = mainClass.newInstance();
    int exit = main.run(args);
    if (!System.getProperty(TEST_LLAMA_JVM_EXIT_SYS_PROP, "false").
        equals("true")) {
      System.exit(exit);
    }
  }

  protected abstract Class<? extends AbstractServer> getServerClass();

  private CountDownLatch runningLatch = new CountDownLatch(1);
  private CountDownLatch stopLatch = new CountDownLatch(1);

  public void shutdown() {
    runningLatch.countDown();
  }

  //Used for testing only
  void waitStopLatch() throws InterruptedException {
    stopLatch.await();

  }

  public int run(String[] args) throws Exception {
    String confDir = System.getProperty(CONF_DIR_SYS_PROP);
    initLogging(confDir);
    logServerInfo();

    LOG.info("Configuration directory: {}", confDir);
    Configuration llamaConf = loadConfiguration(confDir);
    if (args != null) {
      for (String arg : args) {
        if (arg.startsWith("-D")) {
          String[] s = arg.substring(2).split("=");
          if (s.length == 2) {
            llamaConf.set(s[0], s[1]);
          }
        }
      }
    }
    Class<? extends AbstractServer> klass =
        llamaConf.getClass(SERVER_CLASS_KEY, getServerClass(),
            AbstractServer.class);
    LOG.info("Server: {}", klass.getName());
    LOG.info("-----------------------------------------------------------------");
    AbstractServer server = ReflectionUtils.newInstance(klass, llamaConf);

    addShutdownHook(server);

    try {
      server.start();
      runningLatch.await();
      server.stop();
    } catch (Exception ex) {
      LOG.error("Server error: {}", ex.toString(), ex);
      server.stop();
      return 1;
    }
    stopLatch.countDown();
    return server.getExitCode();
  }

  private void initLogging(String confDir) {
    if (System.getProperty("log4j.configuration") == null) {
      System.setProperty("log4j.defaultInitOverride", "true");
      boolean fromClasspath = true;
      File log4jConf = new File(confDir, LOG4J_PROPERTIES).getAbsoluteFile();
      if (log4jConf.exists()) {
        PropertyConfigurator.configureAndWatch(log4jConf.getPath(), 1000);
        fromClasspath = false;
      } else {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL log4jUrl = cl.getResource(LOG4J_PROPERTIES);
        if (log4jUrl != null) {
          PropertyConfigurator.configure(log4jUrl);
        }
      }
      LOG = LoggerFactory.getLogger(this.getClass());
      LOG.debug("Llama log starting");
      if (fromClasspath) {
        LOG.warn("Log4j configuration file '{}' not found", LOG4J_PROPERTIES);
        LOG.warn("Logging with INFO level to standard output");
      }
    }
  }


  public static void logServerInfo() {
    if (LOG == null) {
      LOG = LoggerFactory.getLogger(AbstractMain.class);
    }
    LOG.info("-----------------------------------------------------------------");
    LOG.info("  Java runtime version : {}",
        System.getProperty("java.runtime.version"));
    LOG.info("  Llama version        : {}", VersionInfo.getVersion());
    LOG.info("  Llama built date     : {}", VersionInfo.getBuiltDate());
    LOG.info("  Llama built by       : {}", VersionInfo.getBuiltBy());
    LOG.info("  Llama SCM URI        : {}", VersionInfo.getSCMURI());
    LOG.info("  Llama SCM revision   : {}", VersionInfo.getSCMRevision());
    LOG.info("  Llama source MD5     : {}", VersionInfo.getSourceMD5());
    LOG.info("  Hadoop version       : {}", VersionInfo.getHadoopVersion());
    LOG.info("-----------------------------------------------------------------");
  }

  private static Configuration loadConfiguration(String confDir) {
    Configuration llamaConf = new Configuration(false);
    confDir = (confDir != null) ? confDir : "";
    File file = new File(confDir, SITE_XML);
    if (!file.exists()) {
      LOG.warn("Llama configuration file '{}' not found in '{}'", SITE_XML,
          confDir);
    } else {
      llamaConf.addResource(new Path(file.getAbsolutePath()));
    }
    llamaConf.set(CONF_DIR_SYS_PROP, confDir);
    return llamaConf;
  }

  private static void addShutdownHook(final AbstractServer server) {
    if (!System.getProperty(TEST_LLAMA_JVM_EXIT_SYS_PROP,
        "false").equals("true")) {
      Runtime.getRuntime().addShutdownHook(new Thread("llama-shutdownhook") {
        @Override
        public void run() {
          server.shutdown(0);
        }
      });
    }
  }
}

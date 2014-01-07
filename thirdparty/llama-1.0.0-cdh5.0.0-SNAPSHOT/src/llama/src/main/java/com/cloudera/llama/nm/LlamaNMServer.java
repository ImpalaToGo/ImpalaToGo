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
import com.cloudera.llama.server.ServerConfiguration;
import com.cloudera.llama.server.ThriftServer;
import com.cloudera.llama.thrift.LlamaNMService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.thrift.TProcessor;

import java.io.File;
import java.net.URL;

public class LlamaNMServer extends
    ThriftServer<LlamaNMService.Processor, TProcessor> {
  private Configuration conf;
  private ClientNotificationService clientNotificationService;
  private Resource totalCapacity;

  protected LlamaNMServer() {
    super("LlamaNM", NMServerConfiguration.class);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = new Configuration(conf);
    URL url = Thread.currentThread().getContextClassLoader().
        getResource("yarn-site.xml");
    if (url == null) {
      throw new RuntimeException("'yarn-site.xml' file not found in classpath");
    }
    if (!url.getProtocol().equals("file")) {
      throw new RuntimeException("File 'yarn-site.xml' is in a JAR, it should" +
          " be in a directory");
    }
    String confDir = new File(url.getPath()).getParent();
    conf.set(ServerConfiguration.CONFIG_DIR_KEY, confDir);
    super.setConf(conf);
  }

  @Override
  protected void startService() {
    int memoryMb = conf.getInt(YarnConfiguration.NM_PMEM_MB,
        YarnConfiguration.DEFAULT_NM_PMEM_MB);
    int virtualCores = conf.getInt(YarnConfiguration.NM_VCORES,
        YarnConfiguration.DEFAULT_NM_VCORES);
    totalCapacity = Resource.newInstance(memoryMb, virtualCores);

    try {
      clientNotificationService = new ClientNotificationService(getServerConf(),
          null, getMetricRegistry());
      clientNotificationService.start();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected void stopService() {
    clientNotificationService.stop();
  }

  @Override
  protected LlamaNMService.Processor createServiceProcessor() {
    LlamaNMService.Iface handler = new LlamaNMServiceImpl(
        clientNotificationService);
    return new LlamaNMService.Processor<LlamaNMService.Iface>(handler);
  }

}

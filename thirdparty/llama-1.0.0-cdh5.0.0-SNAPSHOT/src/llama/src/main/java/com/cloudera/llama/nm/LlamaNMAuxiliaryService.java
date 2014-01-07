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

import com.cloudera.llama.server.AbstractMain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class LlamaNMAuxiliaryService extends AuxiliaryService {
  private static final Logger LOG = LoggerFactory.getLogger(LlamaNMServer.class);

  private LlamaNMServer nmServer;

  protected LlamaNMAuxiliaryService() {
    super("llama_nm_plugin");
  }

  @Override
  protected void setConfig(Configuration conf) {
    super.setConfig(conf);
  }

  @Override
  protected synchronized void serviceStart() throws Exception {
    AbstractMain.logServerInfo();

    Configuration llamaConf = new Configuration(getConfig());
    llamaConf.addResource("llama-site.xml");
    LOG.info("Server: {}", LlamaNMServer.class.getName());
    LOG.info("-----------------------------------------------------------------");
    nmServer = new LlamaNMServer();
    nmServer.setConf(llamaConf);
    nmServer.start();
  }

  synchronized LlamaNMServer getNMServer() {
    return nmServer;
  }

  @Override
  public synchronized void serviceStop() {
    if (nmServer != null) {
      nmServer.stop();
      nmServer = null;
    }
    super.stop();
  }

  @Override
  public void initializeApplication(
      ApplicationInitializationContext applicationInitializationContext) {
  }

  @Override
  public void stopApplication(
      ApplicationTerminationContext applicationTerminationContext) {
  }

  @Override
  public ByteBuffer getMetaData() {
    return null;
  }
}

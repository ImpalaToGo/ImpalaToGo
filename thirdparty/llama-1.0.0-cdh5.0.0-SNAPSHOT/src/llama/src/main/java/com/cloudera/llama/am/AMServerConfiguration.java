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
package com.cloudera.llama.am;

import com.cloudera.llama.server.ServerConfiguration;
import org.apache.hadoop.conf.Configuration;

public class AMServerConfiguration extends ServerConfiguration {
  public static final String KEY = "am";

  public AMServerConfiguration() {
    super(KEY);
  }

  public AMServerConfiguration(Configuration conf) {
    super(KEY, conf);
  }

  private static int SERVER_PORT_DEFAULT = 15000;

  @Override
  public int getThriftDefaultPort() {
    return SERVER_PORT_DEFAULT;
  }

  private static int SERVER_ADMIN_PORT_DEFAULT = 15002;

  @Override
  public int getAdminThriftDefaultPort() {
    return SERVER_ADMIN_PORT_DEFAULT;
  }

  private static int HTTP_PORT_DEFAULT = 15001;

  @Override
  public int getHttpDefaultPort() {
    return HTTP_PORT_DEFAULT;
  }

}

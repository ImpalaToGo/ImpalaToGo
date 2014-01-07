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
import com.cloudera.llama.util.FastFormat;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public abstract class ServerConfiguration implements Configurable {
  public static String CONFIG_DIR_KEY = AbstractMain.CONF_DIR_SYS_PROP;

  private String key;
  private Configuration conf;

  protected ServerConfiguration(String key) {
    this.key = key;
    this.conf = new Configuration(false);
  }

  protected ServerConfiguration(String key, Configuration conf) {
    this.key = key;
    this.conf = conf;
  }

  public String getKey() {
    return key;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public String getPropertyName(String nameTemplate) {
    return FastFormat.format(nameTemplate, key);
  }

  public String getConfDir() {
    return conf.get(CONFIG_DIR_KEY, null);
  }

  public static String KEY_PREFIX = "llama.{}.server.thrift.";

  public static String SERVER_MIN_THREADS_KEY = KEY_PREFIX +
      "server.min.threads";
  private static int SERVER_MIN_THREADS_DEFAULT = 10;

  public int getServerMinThreads() {
    return conf.getInt(getPropertyName(SERVER_MIN_THREADS_KEY),
        SERVER_MIN_THREADS_DEFAULT);
  }

  public static String SERVER_MAX_THREADS_KEY = KEY_PREFIX +
      "server.max.threads";
  private static int SERVER_MAX_THREADS_DEFAULT = 50;

  public int getServerMaxThreads() {
    return conf.getInt(getPropertyName(SERVER_MAX_THREADS_KEY),
        SERVER_MAX_THREADS_DEFAULT);
  }

  public static String SECURITY_ENABLED_KEY = KEY_PREFIX + "security";
  private static boolean SECURITY_ENABLED_DEFAULT = false;

  public boolean getSecurityEnabled() {
    return conf.getBoolean(getPropertyName(SECURITY_ENABLED_KEY),
        SECURITY_ENABLED_DEFAULT);
  }

  public static String THRIFT_QOP_KEY = KEY_PREFIX + "security.QOP";
  private static String THRIFT_QOP_DEFAULT = "auth";

  public String getThriftQOP() {
    String qop = conf.get(getPropertyName(THRIFT_QOP_KEY), THRIFT_QOP_DEFAULT);
    if (!qop.equals("auth")
        && !qop.equals("auth-int")
        && !qop.equals("auth-conf")) {
      throw new RuntimeException(FastFormat.format("Invalid Thrift QOP '{}', " +
          "it must be 'auth', 'auth-int' or 'auth-conf'"));
    }
    return qop;
  }

  public static String SERVER_ADDRESS_KEY = KEY_PREFIX + "address";
  private static String SERVER_ADDRESS_DEFAULT = "0.0.0.0";

  public String getThriftAddress() {
    return conf.get(getPropertyName(SERVER_ADDRESS_KEY),
        SERVER_ADDRESS_DEFAULT);
  }

  public abstract int getThriftDefaultPort();

  public static String SERVER_ADMIN_ADDRESS_KEY = KEY_PREFIX + "admin.address";
  private static String SERVER_ADMIN_ADDRESS_DEFAULT = "localhost";

  public String getAdminThriftAddress() {
    return conf.get(getPropertyName(SERVER_ADMIN_ADDRESS_KEY),
        SERVER_ADMIN_ADDRESS_DEFAULT);
  }

  public int getAdminThriftDefaultPort() {
    return -1;
  }

  public static String HTTP_ADDRESS_KEY = KEY_PREFIX + "http.address";
  private static String HTTP_ADDRESS_DEFAULT = "0.0.0.0";

  public String getHttpAddress() {
    return conf.get(getPropertyName(HTTP_ADDRESS_KEY),
        HTTP_ADDRESS_DEFAULT);
  }

  public abstract int getHttpDefaultPort();

  public static String CLIENT_NOTIFIER_QUEUE_THRESHOLD_KEY = KEY_PREFIX +
      "client.notifier.queue.threshold";
  private static int CLIENT_NOTIFIER_QUEUE_THRESHOLD_DEFAULT = 10000;

  public int getClientNotifierQueueThreshold() {
    return conf.getInt(getPropertyName(CLIENT_NOTIFIER_QUEUE_THRESHOLD_KEY),
        CLIENT_NOTIFIER_QUEUE_THRESHOLD_DEFAULT);
  }

  public static String CLIENT_NOTIFIER_THREADS_KEY = KEY_PREFIX +
      "client.notifier.threads";
  private static int CLIENT_NOTIFER_THREADS_DEFAULT = 10;

  public int getClientNotifierThreads() {
    return conf.getInt(getPropertyName(CLIENT_NOTIFIER_THREADS_KEY),
        CLIENT_NOTIFER_THREADS_DEFAULT);
  }

  public static String CLIENT_NOTIFIER_MAX_RETRIES_KEY = KEY_PREFIX +
      "client.notifier.max.retries";
  private static int CLIENT_NOTIFIER_MAX_RETRIES_DEFAULT = 5;

  public int getClientNotifierMaxRetries() {
    return conf.getInt(getPropertyName(CLIENT_NOTIFIER_MAX_RETRIES_KEY),
        CLIENT_NOTIFIER_MAX_RETRIES_DEFAULT);
  }

  public static String CLIENT_NOTIFIER_RETRY_INTERVAL_KEY = KEY_PREFIX +
      "client.notifier.retry.interval.ms";
  private static int CLIENT_NOTIFIER_RETRY_INTERVAL_DEFAULT = 5000;

  public int getClientNotifierRetryInterval() {
    return conf.getInt(getPropertyName(CLIENT_NOTIFIER_RETRY_INTERVAL_KEY),
        CLIENT_NOTIFIER_RETRY_INTERVAL_DEFAULT);
  }

  public static String CLIENT_NOTIFIER_HEARTBEAT_KEY = KEY_PREFIX +
      "client.notifier.heartbeat.ms";
  private static int CLIENT_NOTIFIER_HEARTBEAT_DEFAULT = 30000;

  public int getClientNotifierHeartbeat() {
    return conf.getInt(getPropertyName(CLIENT_NOTIFIER_HEARTBEAT_KEY),
        CLIENT_NOTIFIER_HEARTBEAT_DEFAULT);
  }

  public static String NODE_NAME_MAPPING_CLASS_KEY = KEY_PREFIX +
      "node.name.mapping.class";
  private static Class<? extends NodeMapper> NODE_NAME_MAPPING_CLASS_DEFAULT =
      HostnameOnlyNodeMapper.class;

  public Class<? extends NodeMapper> getNodeMappingClass() {
    return conf.getClass(getPropertyName(NODE_NAME_MAPPING_CLASS_KEY),
        NODE_NAME_MAPPING_CLASS_DEFAULT, NodeMapper.class);
  }

  public static String TRANSPORT_TIMEOUT_KEY = KEY_PREFIX +
      "transport.timeout.ms";
  private static int TRANSPORT_TIMEOUT_DEFAULT = 3600000; // 1hr

  public int getTransportTimeOut() {
    return conf.getInt(getPropertyName(TRANSPORT_TIMEOUT_KEY),
        TRANSPORT_TIMEOUT_DEFAULT);
  }

  public static String KEYTAB_FILE_KEY = KEY_PREFIX + "kerberos.keytab.file";
  private static String KEYTAB_FILE_DEFAULT = "llama.keytab";

  public String getKeytabFile() {
    return conf.get(getPropertyName(KEYTAB_FILE_KEY),
        KEYTAB_FILE_DEFAULT);
  }

  public static String SERVER_PRINCIPAL_NAME_KEY = KEY_PREFIX +
      "kerberos.server.principal.name";
  private static String SERVER_PRINCIPAL_NAME_DEFAULT = "llama/localhost";

  public String getServerPrincipalName() {
    return conf.get(getPropertyName(SERVER_PRINCIPAL_NAME_KEY),
        SERVER_PRINCIPAL_NAME_DEFAULT);
  }

  public static String NOTIFICATION_PRINCIPAL_NAME_KEY = KEY_PREFIX +
      "kerberos.notification.principal.name";
  private static String NOTIFICATION_PRINCIPAL_NAME_DEFAULT = "impala";

  public String getNotificationPrincipalName() {
    return conf.get(getPropertyName(NOTIFICATION_PRINCIPAL_NAME_KEY),
        NOTIFICATION_PRINCIPAL_NAME_DEFAULT);
  }

  public static String ACL_DEFAULT = "*";

  private String[] getACL(String key, boolean users) {
    String[] ret = null;
    String acl = conf.get(getPropertyName(key), ACL_DEFAULT).trim();
    if (!acl.equals(ACL_DEFAULT)) {
      ret = acl.split(" ", 2);
      int index = (users) ? 0 : 1;
      if (index < ret.length) {
        ret = ret[index].split(",");
        for (int i = 0; i < ret.length; i++) {
          ret[i] = ret[i].trim();
        }
      }
    }
    return ret;
  }

  public static String CLIENT_ACL_KEY = KEY_PREFIX + "client.acl";

  public String[] getClientUserACL() {
    return getACL(CLIENT_ACL_KEY, true);
  }

  public String[] getClientGroupACL() {
    return getACL(CLIENT_ACL_KEY, false);
  }

  public static String ADMIN_ACL_KEY = KEY_PREFIX + "admin.acl";

  public String[] getAdminUserACL() {
    return getACL(ADMIN_ACL_KEY, true);
  }

  public String[] getAdminGroupACL() {
    return getACL(ADMIN_ACL_KEY, false);
  }

  public static String LOGGER_SERVLET_READ_ONLY = KEY_PREFIX +
      "loggers.servlet.read.only";
  private static boolean LOGGER_SERVLET_READ_DEFAULT = true;

  public boolean getLoggerServletReadOnly() {
    return conf.getBoolean(getPropertyName(LOGGER_SERVLET_READ_ONLY),
        LOGGER_SERVLET_READ_DEFAULT);
  }


}

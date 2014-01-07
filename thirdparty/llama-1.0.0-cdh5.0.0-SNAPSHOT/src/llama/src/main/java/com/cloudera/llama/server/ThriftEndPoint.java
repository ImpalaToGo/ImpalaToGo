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

import com.cloudera.llama.util.FastFormat;
import org.apache.hadoop.net.NetUtils;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import javax.security.sasl.Sasl;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class ThriftEndPoint {

  public static TTransport createClientTransport(ServerConfiguration conf,
      String host, int port) throws Exception {
    int timeout = conf.getTransportTimeOut();

    TTransport tTransport = new TSocket(host, port, timeout);
    if (Security.isSecure(conf)) {
      String serviceName = conf.getNotificationPrincipalName();
      Map<String, String> saslProperties = new HashMap<String, String>();
      saslProperties.put(Sasl.QOP, "auth-conf,auth-int,auth");
      tTransport = new TSaslClientTransport("GSSAPI", null, serviceName, host,
          saslProperties, null, tTransport);
    }
    return tTransport;
  }

  public static TProcessor createTProcessorWrapper(ServerConfiguration conf,
      boolean isAdmin, TProcessor tProcessor) {
    if (Security.isSecure(conf)) {
      tProcessor = new ClientPrincipalTProcessor(tProcessor);
      tProcessor = new AuthzTProcessor(conf, isAdmin, tProcessor);
    }
    return tProcessor;
  }

  public static TServerSocket createTServerSocket(ServerConfiguration conf)
      throws Exception {
    String strAddress = conf.getThriftAddress();
    int timeout = conf.getTransportTimeOut();
    int defaultPort = conf.getThriftDefaultPort();
    InetSocketAddress address = NetUtils.createSocketAddr(strAddress,
        defaultPort);
    return new TServerSocket(address, timeout);
  }

  public static TServerSocket createAdminTServerSocket(ServerConfiguration conf)
      throws Exception {
    String strAddress = conf.getAdminThriftAddress();
    int timeout = 2000;
    int defaultPort = conf.getAdminThriftDefaultPort();
    InetSocketAddress address = NetUtils.createSocketAddr(strAddress,
        defaultPort);
    return new TServerSocket(address, timeout);
  }

  /**
   * Extracts name from name, name/host@REALM or name/host.
   */
  static String extractPrincipalName(String principal) {
    String name = principal;
    int i = principal.indexOf("/");
    if (i == -1) {
      i = principal.indexOf("@");
    }
    if (i > -1) {
      name = principal.substring(0, i);
    }
    return name;
  }

  /**
   * Extracts host from name, name/host@REALM or name/host.
   */
  static String extractPrincipalHost(String principal) {
    String host = null;
    int i = principal.indexOf("/");
    if (i > -1) {
      int j = principal.lastIndexOf("@");
      if (j > -1) {
        host = principal.substring(i + 1, j);
      } else {
        host = principal.substring(i + 1);
      }
    }
    return host;
  }

  public static TTransportFactory createTTransportFactory(
      ServerConfiguration conf) {
    TTransportFactory factory;
    if (Security.isSecure(conf)) {
      Map<String, String> saslProperties = new HashMap<String, String>();
      saslProperties.put(Sasl.QOP, conf.getThriftQOP());
      String principal = conf.getServerPrincipalName();
      String name = extractPrincipalName(principal);
      String host = extractPrincipalHost(principal);
      if (host == null) {
        throw new IllegalArgumentException(FastFormat.format(
            "Kerberos principal '{}' must have a hostname part", principal));
      }
      TSaslServerTransport.Factory saslFactory = 
          new TSaslServerTransport.Factory();
      saslFactory.addServerDefinition("GSSAPI", name, host, saslProperties, 
          new GssCallback());
      factory = saslFactory;
    } else {
      factory = new TTransportFactory();
    }
    return factory;
  }

  public static String getServerAddress(ServerConfiguration conf) {
    String strAddress = conf.getThriftAddress();
    int defaultPort = conf.getThriftDefaultPort();
    InetSocketAddress address = NetUtils.createSocketAddr(strAddress,
        defaultPort);
    return address.getHostName();
  }

  public static int getServerPort(ServerConfiguration conf) {
    String strAddress = conf.getThriftAddress();
    int defaultPort = conf.getThriftDefaultPort();
    InetSocketAddress address = NetUtils.createSocketAddr(strAddress,
        defaultPort);
    return address.getPort();
  }

}

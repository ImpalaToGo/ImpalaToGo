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
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Security {

  private static String getKrb5LoginModuleName() {
    return System.getProperty("java.vendor").contains("IBM")
           ? "com.ibm.security.auth.module.Krb5LoginModule"
           : "com.sun.security.auth.module.Krb5LoginModule";
  }

  public static class KeytabKerberosConfiguration extends Configuration {
    private String principal;
    private String keytab;
    private boolean isInitiator;

    public KeytabKerberosConfiguration(String principal, File keytab,
        boolean client) {
      this.principal = principal;
      this.keytab = keytab.getAbsolutePath();
      this.isInitiator = client;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();
      options.put("keyTab", keytab);
      options.put("principal", principal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("doNotPrompt", "true");
      options.put("useTicketCache", "true");
      options.put("renewTGT", "true");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", Boolean.toString(isInitiator));
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        options.put("ticketCache", ticketCache);
      }
      options.put("debug", System.getProperty("sun.security.krb5.debug=true",
          "false"));

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              options)};
    }
  }

  private static class KinitKerberosConfiguration extends Configuration {

    private KinitKerberosConfiguration() {
    }

    public static Configuration createClientConfig() {
      return new KinitKerberosConfiguration();
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();
      options.put("useKeyTab", "false");
      options.put("storeKey", "false");
      options.put("doNotPrompt", "true");
      options.put("useTicketCache", "true");
      options.put("renewTGT", "true");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", "true");
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        options.put("ticketCache", ticketCache);
      }
      options.put("debug", System.getProperty("sun.security.krb5.debug=true",
          "false"));

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              options)};
    }
  }

  public static boolean isSecure(ServerConfiguration conf) {
    return conf.getSecurityEnabled();
  }

  private static final Map<Subject, LoginContext> SUBJECT_LOGIN_CTX_MAP =
      new ConcurrentHashMap<Subject, LoginContext>();

  static Subject loginSubject(ServerConfiguration conf, boolean isClient)
      throws Exception {
    Subject subject;
    if (isSecure(conf)) {
      String principalName = conf.getServerPrincipalName();
      String keytab = conf.getKeytabFile();
      if (!(keytab.charAt(0) == '/')) {
        String confDir = conf.getConfDir();
        keytab = new File(confDir, keytab).getAbsolutePath();
      }
      File keytabFile = new File(keytab);
      if (!keytabFile.exists()) {
        throw new RuntimeException(FastFormat.format(
            "Keytab file '{}' does not exist", keytabFile));
      }
      Set<Principal> principals = new HashSet<Principal>();
      principals.add(new KerberosPrincipal(principalName));
      subject = new Subject(false, principals, new HashSet<Object>(),
          new HashSet<Object>());
      LoginContext context = new LoginContext("", subject, null,
          new KeytabKerberosConfiguration(principalName, keytabFile, isClient));
      context.login();
      subject = context.getSubject();
      SUBJECT_LOGIN_CTX_MAP.put(subject, context);
    } else {
      subject = new Subject();
    }
    return subject;
  }

  public static Subject loginClientFromKinit() throws Exception {
    LoginContext context = new LoginContext("", new Subject(), null,
        KinitKerberosConfiguration.createClientConfig());
    context.login();
    Subject subject = context.getSubject();
    SUBJECT_LOGIN_CTX_MAP.put(subject, context);
    return subject;
  }
  
  public static Subject loginServerSubject(ServerConfiguration conf)
      throws Exception {
    return loginSubject(conf, false);
  }

  public static Subject loginClientSubject(ServerConfiguration conf)
      throws Exception {
    return loginSubject(conf, true);
  }

  public static void logout(Subject subject) {
    LoginContext loginContext = SUBJECT_LOGIN_CTX_MAP.remove(subject);
    if (loginContext != null) {
      try {
        loginContext.logout();
      } catch (LoginException ex) {
        //TODO LOG
      }
    }
  }

  public static void loginToHadoop(ServerConfiguration conf) throws Exception {
    if (UserGroupInformation.isSecurityEnabled()) {
      String principalName = conf.getServerPrincipalName();
      String keytab = conf.getKeytabFile();
      if (!(keytab.charAt(0) == '/')) {
        String confDir = conf.getConfDir();
        keytab = new File(confDir, keytab).getAbsolutePath();
      }
      File keytabFile = new File(keytab).getAbsoluteFile();
      if (!keytabFile.exists()) {
        throw new RuntimeException(FastFormat.format(
            "Keytab file '{}' does not exist", keytabFile));
      }
      UserGroupInformation.loginUserFromKeytab(principalName,
          keytabFile.getPath());
    }
  }

}

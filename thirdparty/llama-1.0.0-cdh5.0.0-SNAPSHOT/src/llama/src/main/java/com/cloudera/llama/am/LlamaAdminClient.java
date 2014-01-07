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

import com.cloudera.llama.server.Security;
import com.cloudera.llama.server.TypeUtils;
import com.cloudera.llama.thrift.LlamaAMAdminService;
import com.cloudera.llama.thrift.TLlamaAMAdminEmptyCacheRequest;
import com.cloudera.llama.thrift.TLlamaAMAdminEmptyCacheResponse;
import com.cloudera.llama.thrift.TLlamaAMAdminReleaseRequest;
import com.cloudera.llama.thrift.TLlamaAMAdminReleaseResponse;
import com.cloudera.llama.thrift.TLlamaServiceVersion;
import com.cloudera.llama.thrift.TStatusCode;
import com.cloudera.llama.util.CLIParser;
import com.cloudera.llama.util.ErrorCode;
import com.cloudera.llama.util.UUID;
import com.cloudera.llama.util.VersionInfo;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LlamaAdminClient {
  private static final String LLAMAADMIN_CONFIG = "llamaadmin-site.xml";

  private static final String LLAMAADMIN_SERVER_ADDRESS_KEY =
      "llamaadmin.server.thrift.address";
  private static final String LLAMAADMIN_SERVER_SECURE_KEY =
      "llamaadmin.server.thrift.secure";

  private static final String LLAMAADMIN_SERVER_ADDRESS_DEFAULT = "localhost";
  private static final int LLAMAADMIN_SERVER_PORT_DEFAULT = 15002;
  private static final boolean LLAMAADMIN_SERVER_SECURE_DEFAULT = false;

  private static final String HELP_CMD = "help";
  private static final String RELEASE_CMD = "release";
  private static final String ERROR_CODES_CMD = "errorcodes";
  private static final String EMPTY_CACHE_CMD = "emptycache";
  private static final String VERSION_CMD = "version";

  private static final String LLAMA = "llama";
  private static final String SECURE = "secure";
  private static final String HANDLES = "handles";
  private static final String QUEUES = "queues";
  private static final String RESERVATIONS = "reservations";
  private static final String DO_NOT_CACHE = "donotcache";
  private static final String ALL_QUEUES = "allqueues";

  private static CLIParser createParser() {
    CLIParser parser = new CLIParser("llamaadmin", new String[0]);

    Option llama = new Option(LLAMA, true, "<HOST>:<PORT> of llama");
    llama.setRequired(false);
    Option secure = new Option(SECURE, false, "uses kerberos");
    secure.setRequired(false);
    Option handle = new Option(HANDLES, true,
        "client handles (comma separated)");
    handle.setRequired(false);
    Option queue = new Option(QUEUES, true, "queues (comma separated)");
    queue.setRequired(false);
    Option reservation = new Option(RESERVATIONS, true,
        "reservations (comma separated)");
    reservation.setRequired(false);
    Option doNotCache = new Option(DO_NOT_CACHE, false,
        "do not cache resources of released resources");
    doNotCache.setRequired(false);
    Option allQueues = new Option(ALL_QUEUES, false,
        "empty cache for all queues");
    doNotCache.setRequired(false);

    //help
    Options options = new Options();
    parser.addCommand(HELP_CMD, "",
        "display usage for all commands or specified command", options, false);

    //release
    options = new Options();
    options.addOption(llama);
    options.addOption(secure);
    options.addOption(handle);
    options.addOption(queue);
    options.addOption(reservation);
    options.addOption(doNotCache);
    parser.addCommand(RELEASE_CMD, "",
        "release queues, handles or reservations", options, false);

    //emptycache
    options = new Options();
    options.addOption(llama);
    options.addOption(secure);
    options.addOption(queue);
    options.addOption(allQueues);
    parser.addCommand(EMPTY_CACHE_CMD, "",
        "empty cached resources not in use", options, false);

    //errorcodes
    options = new Options();
    parser.addCommand(ERROR_CODES_CMD, "", "list Llama error codes", options,
        false);

    //version
    options = new Options();
    parser.addCommand(VERSION_CMD, "", "prints version of the llamaadminclient",
        options, false);

    return parser;
  }

  public static void main(String[] args) throws Exception {
    System.exit(execute(args));
  }

  public static int execute(String[] args) throws Exception {
    int exitCode = 1;
    CLIParser parser = createParser();
    try {
      CLIParser.Command command = parser.parse(args);
      CommandLine cl = command.getCommandLine();

      Configuration conf = new Configuration(false);
      conf.addResource(LLAMAADMIN_CONFIG);
      if (cl.hasOption(SECURE)) {
        conf.setBoolean(LLAMAADMIN_SERVER_SECURE_KEY, true);
      }
      if (cl.hasOption(LLAMA)) {
        conf.set(LLAMAADMIN_SERVER_ADDRESS_KEY, cl.getOptionValue(LLAMA));
      }
      String llama = conf.get(LLAMAADMIN_SERVER_ADDRESS_KEY,
          LLAMAADMIN_SERVER_ADDRESS_DEFAULT);
      boolean secure = conf.getBoolean(LLAMAADMIN_SERVER_SECURE_KEY,
          LLAMAADMIN_SERVER_SECURE_DEFAULT);

      if (command.getName().equals(HELP_CMD)) {
        parser.showHelp(command.getCommandLine());
        exitCode = 0;
      } else if (command.getName().equals(RELEASE_CMD)) {
        boolean doNotCache = cl.hasOption(DO_NOT_CACHE);
        List<UUID> handles = optToHandles(cl.getOptionValue(HANDLES));
        List<UUID> reservations = optToHandles(cl.getOptionValue(RESERVATIONS));
        List<String> queues = optToStrings(cl.getOptionValue(QUEUES));

        if (handles.isEmpty() && reservations.isEmpty() && queues.isEmpty()) {
          System.err.print("At least one of the -queues, -handles or " +
              "-reservations options must be specified");
          exitCode = 1;
        } else {
          release(secure, getHost(llama),
              getPort(llama, LLAMAADMIN_SERVER_PORT_DEFAULT), handles,
              reservations, queues, doNotCache);
          exitCode = 0;
        }
      } else if (command.getName().equals(EMPTY_CACHE_CMD)) {
        boolean allQueues = cl.hasOption(ALL_QUEUES);
        List<String> queues = optToStrings(cl.getOptionValue(QUEUES));
        if ((!allQueues && queues.isEmpty()) ||
            (allQueues && !queues.isEmpty())) {
          System.err.print("Either the -allqueues or the -queues option must " +
              "be specified");
          exitCode = 1;
        } else {
          emptyCache(secure, getHost(llama),
              getPort(llama, LLAMAADMIN_SERVER_PORT_DEFAULT), queues, allQueues);
          exitCode = 0;
        }
      } else if (command.getName().equals(ERROR_CODES_CMD)) {
        System.out.println();
        System.out.println("Error codes for Llama version: " + VersionInfo.getVersion());
        System.out.println();
        for (String description : ErrorCode.ERROR_CODE_DESCRIPTIONS) {
          System.out.println("  " + description);
        }
        System.out.println();
      } else if (command.getName().equals(VERSION_CMD)) {
        System.out.println(VersionInfo.getVersion());
      } else {
        System.err.println("Sub-command missing");
        System.err.println();
        System.err.println(parser.shortHelp());
        exitCode = 1;
      }
    } catch (ParseException ex) {
      System.err.println("Invalid sub-command: " + ex.getMessage());
      System.err.println();
      System.err.println(parser.shortHelp());
      exitCode = 1;
    } catch (Throwable ex) {
      System.err.println("Error: " + ex.getMessage());
      ex.printStackTrace(System.err);
      exitCode = 2;
    }
    return exitCode;
  }

  private static List<String> optToStrings(String str) {
    List<String> list = new ArrayList<String>();
    if (str != null) {
      Collections.addAll(list, str.split(","));
    }
    return list;
  }

  private static List<UUID> optToHandles(String str) {
    List<UUID> list = new ArrayList<UUID>();
    if (str != null) {
      for (String value : str.split(",")) {
        list.add(UUID.fromString(value));
      }
    }
    return list;
  }

  private static String getHost(String value) {
    int colon = value.indexOf(":");
    return (colon == -1) ? value : value.substring(0, colon);
  }

  private static int getPort(String value, int defaultPort) {
    int colon = value.indexOf(":");
    return (colon == -1) ? defaultPort
                         : Integer.parseInt(value.substring(colon + 1));
  }

  static Subject getSubject(boolean secure) throws Exception {
    return (secure) ? Security.loginClientFromKinit() : new Subject();
  }

  static TTransport createTransport(boolean secure, String host,
      int port) throws Exception {
    TTransport transport = new TSocket(host, port);
    if (secure) {
      Map<String, String> saslProperties = new HashMap<String, String>();
      saslProperties.put(Sasl.QOP, "auth-conf,auth-int,auth");
      transport = new TSaslClientTransport("GSSAPI", null, "llama", host,
          saslProperties, null, transport);
    }
    transport.open();
    return transport;
  }

  static LlamaAMAdminService.Client createClient(TTransport transport)
      throws Exception {
    TProtocol protocol = new TBinaryProtocol(transport);
    return new LlamaAMAdminService.Client(protocol);
  }

  static void release(final boolean secure, final String llamaHost,
      final int llamaPort, final List<UUID> handles,
      final List<UUID> reservations, final List<String> queues,
      final boolean doNotCache)
      throws Exception {
    Subject.doAs(getSubject(secure),
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            TTransport transport = createTransport(secure, llamaHost,
                llamaPort);
            try {
              LlamaAMAdminService.Client client = createClient(transport);
              TLlamaAMAdminReleaseRequest req = new TLlamaAMAdminReleaseRequest();
              req.setVersion(TLlamaServiceVersion.V1);
              req.setDo_not_cache(doNotCache);
              req.setQueues(queues);
              req.setHandles(TypeUtils.toTUniqueIds(handles));
              req.setReservations(TypeUtils.toTUniqueIds(reservations));
              TLlamaAMAdminReleaseResponse res = client.Release(req);
              if (res.getStatus().getStatus_code() != TStatusCode.OK) {
                throw new RuntimeException(res.toString());
              }
              if (!res.getStatus().getError_msgs().isEmpty()) {
                for (String msg : res.getStatus().getError_msgs()) {
                  System.err.println("  " + msg);
                }
              }
            } finally {
              transport.close();
            }
            return null;
          }
        });
  }

  static void emptyCache(final boolean secure, final String llamaHost,
      final int llamaPort, final List<String> queues,
      final boolean allQueues)
      throws Exception {
    Subject.doAs(getSubject(secure),
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            TTransport transport = createTransport(secure, llamaHost,
                llamaPort);
            try {
              LlamaAMAdminService.Client client = createClient(transport);
              TLlamaAMAdminEmptyCacheRequest req =
                  new TLlamaAMAdminEmptyCacheRequest();
              req.setVersion(TLlamaServiceVersion.V1);
              req.setAllQueues(allQueues);
              req.setQueues(queues);
              TLlamaAMAdminEmptyCacheResponse res = client.EmptyCache(req);
              if (res.getStatus().getStatus_code() != TStatusCode.OK) {
                throw new RuntimeException(res.toString());
              }
              if (!res.getStatus().getError_msgs().isEmpty()) {
                for (String msg : res.getStatus().getError_msgs()) {
                  System.err.println("  " + msg);
                }
              }
            } finally {
              transport.close();
            }
            return null;
          }
        });
  }

}

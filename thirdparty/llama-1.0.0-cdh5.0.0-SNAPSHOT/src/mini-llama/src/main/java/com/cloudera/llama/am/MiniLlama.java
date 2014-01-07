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

import com.cloudera.llama.am.api.LlamaAM;
import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.util.ParamChecker;
import com.cloudera.llama.am.yarn.YarnRMConnector;
import com.cloudera.llama.server.AbstractServer;
import com.cloudera.llama.server.NodeMapper;
import com.cloudera.llama.server.ServerConfiguration;
import com.cloudera.llama.util.CLIParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PatternOptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MiniLlama {

  static {
    System.setProperty("log4j.configuration", "llama-log4j.properties");
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  private static final String HELP_CMD = "help";
  private static final String MINICLUSTER_CMD = "minicluster";
  private static final String CLUSTER_CMD = "cluster";

  private static final String NODES = "nodes";
  private static final String HDFS_NO_FORMAT = "hdfsnoformat";
  private static final String HDFS_WRITE_CONF = "hdfswriteconf";

  private static CLIParser createParser() {
    CLIParser parser = new CLIParser("minillama", new String[0]);

    Option nodes = new Option(NODES, true, "number of nodes (default: 1)");
    nodes.setRequired(false);
    nodes.setType(PatternOptionBuilder.NUMBER_VALUE);
    Option hdfsNoFormat = new Option(HDFS_NO_FORMAT, false,
        "don't format mini HDFS");
    hdfsNoFormat.setRequired(false);
    Option hdfsWriteConf = new Option(HDFS_WRITE_CONF, true,
        "file to write mini HDFS configuration");
    hdfsWriteConf.setRequired(false);

    //help
    Options options = new Options();
    parser.addCommand(HELP_CMD, "",
        "display usage for all commands or specified command", options, false);

    //minicluster
    options = new Options();
    options.addOption(nodes);
    options.addOption(hdfsNoFormat);
    options.addOption(hdfsWriteConf);
    parser.addCommand(MINICLUSTER_CMD, "",
        "start embedded mini HDFS/Yarn cluster", options, false);

    //cluster
    options = new Options();
    parser.addCommand(CLUSTER_CMD, "", "use external HDFS/Yarn cluster",
        options, false);
    return parser;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(false);
    conf.addResource("llama-site.xml");

    CLIParser parser = createParser();
    try {
      CLIParser.Command command = parser.parse(args);
      if (command.getName().equals(HELP_CMD)) {
        parser.showHelp(command.getCommandLine());
      } else {
        final MiniLlama llama;
        if (command.getName().equals(MINICLUSTER_CMD)) {
          CommandLine cl = command.getCommandLine();
          int nodes = Integer.parseInt(cl.getOptionValue(NODES, "1"));
          conf = createMiniLlamaConf(conf, nodes);
          llama = new MiniLlama(conf);
          llama.skipDfsFormat(cl.hasOption(HDFS_NO_FORMAT));
          if (cl.hasOption(HDFS_WRITE_CONF)) {
            llama.setWriteHadoopConfig(cl.getOptionValue(HDFS_WRITE_CONF));
          }
        } else {
          conf.setBoolean(MINI_USE_EXTERNAL_HADOOP_KEY, true);
          conf = createMiniLlamaConf(conf, 1); //nodes is ignored
          llama = new MiniLlama(conf);
        }
        llama.start();
        String clusterType = (command.getName().equals(MINICLUSTER_CMD))
                             ? "external HDFS/Yarn cluster"
                             : "embedded HDFS/Yarn mini-cluster";
        LOG.info("**************************************************************"
            + "*******************************************************");
        LOG.info("Mini Llama running with {} with {} nodes, " +
            "HDFS URI: {} Llama URI: {}", clusterType, llama.getNodes(),
            llama.getHadoopConf().get("fs.defaultFS"),
            llama.getAddressHost() + ":" + llama.getAddressPort());
        LOG.info("*************************************************************" +
            "********************************************************");
        Runtime.getRuntime().addShutdownHook(new Thread("minillama-shutdownhoock") {
          @Override
          public void run() {
            llama.stop();
          }
        });
        synchronized (MiniLlama.class) {
          MiniLlama.class.wait();
        }
      }
    } catch (ParseException ex) {
      System.err.println("Invalid sub-command: " + ex.getMessage());
      System.err.println();
      System.err.println(parser.shortHelp());
      System.exit(1);
    } catch (Throwable ex) {
      System.err.println("Error: " + ex.getMessage());
      ex.printStackTrace(System.err);
      System.exit(2);
    }

  }

  private static ServerConfiguration S_CONF = new AMServerConfiguration(
      new Configuration(false));

  public static Configuration createMiniLlamaConf(Configuration conf,
      int nodes) {
    ParamChecker.notNull(conf, "conf");
    ParamChecker.greaterThan(nodes, 0, "nodes");
    conf.set(ServerConfiguration.CONFIG_DIR_KEY, "");
    conf.setIfUnset(LlamaAM.RM_CONNECTOR_CLASS_KEY, YarnRMConnector
        .class.getName());
    conf.setInt(MINI_CLUSTER_NODES_KEY, nodes);
    conf.setIfUnset(S_CONF.getPropertyName(
        ServerConfiguration.SERVER_ADDRESS_KEY), "localhost:0");
    conf.setIfUnset(S_CONF.getPropertyName(
        ServerConfiguration.SERVER_ADMIN_ADDRESS_KEY), "localhost:0");
    conf.setIfUnset(S_CONF.getPropertyName(
        ServerConfiguration.HTTP_ADDRESS_KEY), "localhost:0");
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
        true);
    return conf;
  }

  public static Configuration createMiniClusterConf(int nodes) {
    return createMiniLlamaConf(new Configuration(false), nodes);
  }

  private static final Logger LOG = LoggerFactory.getLogger(MiniLlama.class);

  public static final String MINI_USE_EXTERNAL_HADOOP_KEY =
      "llama.am.server.mini.use.external.hadoop";

  public static final String MINI_SERVER_CLASS_KEY =
      "llama.am.server.mini.server.class";

  private static final String MINI_CLUSTER_NODES_KEY =
      "llama.am.server.mini.cluster.nodes";

  private final Configuration conf;
  private boolean skipDfsFormat = false;
  private String writeHdfsConfig = null;
  private final AbstractServer server;
  private List<String> dataNodes;
  private MiniDFSCluster miniHdfs;
  private Configuration hadoopConf;
  private MiniYARNCluster miniYarn;
  private boolean useExternalHadoop;
  private int nodes;

  public MiniLlama(Configuration conf) {
    ParamChecker.notNull(conf, "conf");
    Class<? extends AbstractServer> klass = conf.getClass(MINI_SERVER_CLASS_KEY,
        LlamaAMServer.class, AbstractServer.class);
    server = ReflectionUtils.newInstance(klass, conf);
    this.conf = server.getConf();
    useExternalHadoop = conf.getBoolean(MINI_USE_EXTERNAL_HADOOP_KEY, false);
  }

  public Configuration getConf() {
    return conf;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public void skipDfsFormat(boolean skipDfsFormat) {
    this.skipDfsFormat = skipDfsFormat;
  }

  public void setWriteHadoopConfig(String writeHdfsConfig) {
    this.writeHdfsConfig = writeHdfsConfig;
  }

  private Map<String, String> getDataNodeNodeManagerMapping(Configuration conf) throws Exception {
    Map<String, String> map = new HashMap<String, String>();
    DFSClient dfsClient = new DFSClient(new URI(conf.get("fs.defaultFS")), conf);
    DatanodeInfo[] DNs = dfsClient.datanodeReport(HdfsConstants.DatanodeReportType.ALL);
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    List<NodeId> nodeIds = getYarnNodeIds(conf);
    if (nodeIds.size() != DNs.length) {
      throw new RuntimeException("Number of DNs and NMs differ, MiniLlama " +
          "node mapping requires them to be equal at startup");
    }
    LOG.info("HDFS/YARN mapping:");
    for (int i = 0; i < DNs.length; i++) {
      String key = DNs[i].getXferAddr();
      NodeId nodeId = nodeIds.get(i);
      String value = nodeId.getHost() + ":" + nodeId.getPort();
      map.put(key, value);
      LOG.info("  DN/NM: " + key + "/" + value);
    }
    yarnClient.stop();
    nodes = map.size();
    verifySingleHost(map.keySet(), "DataNode");
    verifySingleHost(map.values(), "NodeManager");
    return map;
  }

  private void verifySingleHost(Collection<String> addresses, String nodeType) {
    String host = null;
    for (String address : addresses) {
      String h = address.substring(0, address.indexOf(":"));
      if (host == null) {
        host = h;
      } else if (!host.equals(h)) {
        throw new RuntimeException(
            FastFormat.format("Cluster {}s are running in the same host: {}",
                nodeType, addresses));
      }
    }
  }

  public void start() throws Exception {
    if (useExternalHadoop) {
      hadoopConf = new YarnConfiguration();
    } else {
      hadoopConf = startMiniHadoop();
      if (writeHdfsConfig != null) {
        FileOutputStream fos = new FileOutputStream(new File(writeHdfsConfig));
        hadoopConf.writeXml(fos);
        fos.close();
      }
    }
    server.getConf().setClass(S_CONF.getPropertyName(
        ServerConfiguration.NODE_NAME_MAPPING_CLASS_KEY),
        MiniClusterNodeMapper.class, NodeMapper.class);
    Map<String, String> mapping = getDataNodeNodeManagerMapping(hadoopConf);
    MiniClusterNodeMapper.addMapping(server.getConf(), mapping);
    for (Map.Entry entry : hadoopConf) {
      server.getConf().set((String) entry.getKey(), (String) entry.getValue());
    }
    dataNodes = new ArrayList<String>(mapping.keySet());
    dataNodes = Collections.unmodifiableList(dataNodes);
    server.start();
  }

  public void stop() {
    server.stop();
    if (useExternalHadoop) {
      stopMiniHadoop();
    }
  }

  public int getNodes() {
    return nodes;
  }

  public String getAddressHost() {
    return server.getAddressHost();
  }

  public int getAddressPort() {
    return server.getAddressPort();
  }

  public List<String> getDataNodes() {
    return dataNodes;
  }

  private Configuration startMiniHadoop() throws Exception {
    int clusterNodes = getConf().getInt(MINI_CLUSTER_NODES_KEY, 1);
    if (System.getProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA) == null) {
      String testBuildData = new File("target").getAbsolutePath();
      System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, testBuildData);
    }
    //to trigger hdfs-site.xml registration as default resource
    new HdfsConfiguration();
    Configuration conf = new YarnConfiguration();
    String llamaProxyUser = System.getProperty("user.name");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hadoop.proxyuser." + llamaProxyUser + ".hosts", "*");
    conf.set("hadoop.proxyuser." + llamaProxyUser + ".groups", "*");
    String[] userGroups = new String[]{"g"};
    UserGroupInformation.createUserForTesting(llamaProxyUser, userGroups);

    int hdfsPort = 0;
    String fsUri = conf.get("fs.defaultFS");
    if (fsUri != null && !fsUri.equals("file:///")) {
      int i = fsUri.lastIndexOf(":");
      if (i > -1) {
        try {
          hdfsPort = Integer.parseInt(fsUri.substring(i + 1));
        } catch (Exception ex) {
          throw new RuntimeException("Could not parse port from Hadoop's " +
              "'fs.defaultFS property: " + fsUri);
        }
      }
    }
    miniHdfs = new MiniDFSCluster(hdfsPort, conf, clusterNodes, !skipDfsFormat,
        true, null, null);
    miniHdfs.waitActive();
    conf = miniHdfs.getConfiguration(0);
    miniYarn = new MiniYARNCluster("minillama", clusterNodes, 1, 1);
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
        true);
    miniYarn.init(conf);
    miniYarn.start();
    conf = miniYarn.getConfig();

    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    return conf;
  }

  private List<NodeId> getYarnNodeIds(Configuration conf) throws Exception {
    List<NodeId> list = new ArrayList<NodeId>();
    if (miniYarn != null) {
      int clusterNodes = getConf().getInt(MINI_CLUSTER_NODES_KEY, 1);
      for (int i = 0; i < clusterNodes; i++) {
        list.add(miniYarn.getNodeManager(i).getNMContext().getNodeId());
      }
    } else {
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(conf);
      yarnClient.start();
      List<NodeReport> nodes = yarnClient.getNodeReports(NodeState.RUNNING);
      for (int i = 0; i < nodes.size(); i++) {
        list.add(nodes.get(i).getNodeId());
      }
      yarnClient.stop();
    }
    return list;
  }

  private void stopMiniHadoop() {
    if (miniYarn != null) {
      miniYarn.stop();
      miniYarn = null;
    }
    if (miniHdfs != null) {
      miniHdfs.shutdown();
      miniHdfs = null;
    }
  }

}

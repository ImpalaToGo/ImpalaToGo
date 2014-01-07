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

import com.cloudera.llama.util.FastFormat;
import com.cloudera.llama.server.NodeMapper;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MiniClusterNodeMapper implements NodeMapper, Configurable {

  public static final String MAPPING_KEY =
      "llama.minicluster.node.mapper.mapping";

  public static void addMapping(Configuration conf,
      Map<String, String> mapping) {
    try {
      Properties props = new Properties();
      props.putAll(mapping);
      StringWriter writer = new StringWriter();
      props.store(writer, "");
      writer.close();
      conf.set(MAPPING_KEY, writer.toString());
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
  }

  private Configuration conf;
  private Map<String, String> dn2nm;
  private Map<String, String> nm2dn;

  @Override
  @SuppressWarnings("unchecked")
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      String str = conf.get(MAPPING_KEY);
      if (str != null) {
        StringReader reader = new StringReader(str);
        Properties props = new Properties();
        props.load(reader);
        dn2nm = new HashMap<String, String>((Map) props);
        nm2dn = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : dn2nm.entrySet()) {
          nm2dn.put(entry.getValue(), entry.getKey());
        }
      } else {
        throw new RuntimeException(FastFormat.format(
            "Mapping property '{}' not set in the configuration", MAPPING_KEY));
      }
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public String getNodeManager(String dataNode) {
    String name = dn2nm.get(dataNode);
    if (name == null) {
      throw new IllegalArgumentException(FastFormat.format(
          "DataNode '{}' does not have a mapping", dataNode));
    }
    return name;
  }

  @Override
  public List<String> getNodeManagers(List<String> dataNodes) {
    List<String> list = new ArrayList<String>();
    for (String dataNode : dataNodes) {
      list.add(getNodeManager(dataNode));
    }
    return list;
  }

  @Override
  public String getDataNode(String nodeManager) {
    String name = nm2dn.get(nodeManager);
    if (name == null) {
      throw new IllegalArgumentException(FastFormat.format(
          "NodeManager '{}' does not have a mapping", nodeManager));
    }
    return name;
  }

  @Override
  public List<String> getDataNodes(List<String> nodeManagers) {
    List<String> list = new ArrayList<String>();
    for (String nodeManager : nodeManagers) {
      list.add(getDataNode(nodeManager));
    }
    return list;
  }

}

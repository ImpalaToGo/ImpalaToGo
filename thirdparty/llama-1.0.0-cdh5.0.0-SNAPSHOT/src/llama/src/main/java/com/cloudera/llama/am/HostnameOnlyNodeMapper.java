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

import com.cloudera.llama.server.NodeMapper;

import java.util.ArrayList;
import java.util.List;

public class HostnameOnlyNodeMapper implements NodeMapper {

  private String removePortIfPresent(String name) {
    int index = name.indexOf(":");
    return (index == -1) ? name : name.substring(0, index);
  }

  @Override
  public String getNodeManager(String dataNode) {
    return removePortIfPresent(dataNode);
  }

  @Override
  public List<String> getNodeManagers(List<String> dataNodes) {
    List<String> list = new ArrayList<String>(dataNodes.size());
    for (String node : dataNodes) {
      list.add(getNodeManager(node));
    }
    return list;
  }

  @Override
  public String getDataNode(String nodeManager) {
    return removePortIfPresent(nodeManager);
  }

  @Override
  public List<String> getDataNodes(List<String> nodeManagers) {
    List<String> list = new ArrayList<String>(nodeManagers.size());
    for (String node : nodeManagers) {
      list.add(getDataNode(node));
    }
    return list;
  }

}

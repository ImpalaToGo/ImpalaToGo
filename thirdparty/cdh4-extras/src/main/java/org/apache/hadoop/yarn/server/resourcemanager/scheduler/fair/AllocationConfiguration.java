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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;
//QUTIL: MODIFIED

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

public class AllocationConfiguration {
  private static final AccessControlList EVERYBODY_ACL = new AccessControlList("*");
  private static final AccessControlList NOBODY_ACL = new AccessControlList(" ");

  @VisibleForTesting
  final Map<String, Resource> maxQueueResources;

  // ACL's for each queue. Only specifies non-default ACL's from configuration.
  private final Map<String, Map<QueueACL, AccessControlList>> queueAcls;

  // Policy for mapping apps to queues
  @VisibleForTesting
  QueuePlacementPolicy placementPolicy;

  public AllocationConfiguration(Map<String, Resource> minQueueResources,
      Map<String, Resource> maxQueueResources,
      Map<String, Integer> queueMaxApps, Map<String, Integer> userMaxApps,
      Map<String, ResourceWeights> queueWeights, int userMaxAppsDefault,
      int queueMaxAppsDefault, Map<String, SchedulingPolicy> schedulingPolicies,
      SchedulingPolicy defaultSchedulingPolicy,
      Map<String, Long> minSharePreemptionTimeouts,
      Map<String, Map<QueueACL, AccessControlList>> queueAcls,
      long fairSharePreemptionTimeout, long defaultMinSharePreemptionTimeout,
      QueuePlacementPolicy placementPolicy, Set<String> queueNames) {
    this.queueAcls = queueAcls;
    this.placementPolicy = placementPolicy;
    this.maxQueueResources = maxQueueResources;
  }

  public AllocationConfiguration(Configuration conf) {
    queueAcls = new HashMap<String, Map<QueueACL, AccessControlList>>();
    placementPolicy = QueuePlacementPolicy.fromConfiguration(conf,
        new HashSet<String>());
    maxQueueResources = new HashMap<String, Resource>();
  }

  /**
   * Get the maximum resource allocation for the given queue.
   * @return the cap set on this queue, or Integer.MAX_VALUE if not set.
   */
  public Resource getMaxResources(String queueName) {
    Resource maxQueueResource = maxQueueResources.get(queueName);
    return (maxQueueResource == null) ? Resources.unbounded() : maxQueueResource;
  }

  /**
   * Get the ACLs associated with this queue. If a given ACL is not explicitly
   * configured, include the default value for that ACL.  The default for the
   * root queue is everybody ("*") and the default for all other queues is
   * nobody ("")
   */
  public AccessControlList getQueueAcl(String queue, QueueACL operation) {
    Map<QueueACL, AccessControlList> queueAcls = this.queueAcls.get(queue);
    if (queueAcls != null) {
      AccessControlList operationAcl = queueAcls.get(operation);
      if (operationAcl != null) {
        return operationAcl;
      }
    }
    return (queue.equals("root")) ? EVERYBODY_ACL : NOBODY_ACL;
  }


  public boolean hasAccess(String queueName, QueueACL acl,
      UserGroupInformation user) {
    int lastPeriodIndex = queueName.length();
    while (lastPeriodIndex != -1) {
      String queue = queueName.substring(0, lastPeriodIndex);
      if (getQueueAcl(queue, acl).isUserAllowed(user)) {
        return true;
      }

      lastPeriodIndex = queueName.lastIndexOf('.', lastPeriodIndex - 1);
    }

    return false;
  }

  public QueuePlacementPolicy getPlacementPolicy() {
    return placementPolicy;
  }
}

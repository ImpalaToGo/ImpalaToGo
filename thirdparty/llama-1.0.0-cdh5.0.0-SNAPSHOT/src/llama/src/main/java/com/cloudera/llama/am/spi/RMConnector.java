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
package com.cloudera.llama.am.spi;

import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;

import java.util.Collection;
import java.util.List;

public interface RMConnector {

  public void setLlamaAMCallback(RMListener callback);

  public void start() throws LlamaException;

  public void stop();

  public void register(String queue) throws LlamaException;

  public void unregister();

  public List<String> getNodes() throws LlamaException;

  /**
   * Submits requests for the given resources to the RM.
   */
  public void reserve(Collection<RMResource> resources) throws LlamaException;

  /**
   * Releases the given resources by giving back allocated containers
   * to the RM and canceling outstanding requests.
   */
  public void release(Collection<RMResource> resources, boolean doNotCache)
      throws LlamaException;

  /**
   * Used to cache resources.
   */
  public boolean reassignResource(Object rmResourceId, UUID resourceId);

  public void emptyCache() throws LlamaException;

  public void setMetricRegistry(MetricRegistry registry);
}

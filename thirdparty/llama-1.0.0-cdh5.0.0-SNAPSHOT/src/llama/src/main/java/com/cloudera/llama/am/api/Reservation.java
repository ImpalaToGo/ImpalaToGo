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
package com.cloudera.llama.am.api;

import com.cloudera.llama.util.UUID;

import java.util.List;

public interface Reservation {

  public interface Builder {

    public Builder setHandle(UUID handle);

    public Builder setUser(String user);

    public Builder setQueue(String queue);

    public Builder addResource(Resource resource);

    public Builder addResources(List<Resource> resources);

    public Builder setResources(List<Resource> resources);

    public Builder setGang(boolean gang);

    public Reservation build();

  }

  public UUID getHandle();

  public String getUser();

  public String getQueue();

  public List<Resource> getResources();

  public boolean isGang();

}

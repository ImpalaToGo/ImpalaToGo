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

public interface Resource {

  public interface Builder {

    public Builder setResourceId(UUID resourceId);

    public Builder setLocationAsk(String locationAsk);

    public Builder setLocalityAsk(Locality localityAsk);

    public Builder setCpuVCoresAsk(int cpuVCoresAsk);

    public Builder setMemoryMbsAsk(int memoryMbsAsk);

    public Resource build();

  }

  public enum Locality {
    MUST, PREFERRED, DONT_CARE
  }

  public UUID getResourceId();


  public String getLocationAsk();

  public Locality getLocalityAsk();

  public int getCpuVCoresAsk();

  public int getMemoryMbsAsk();

}

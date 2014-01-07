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
package com.cloudera.llama.am.cache;

public class Key implements Comparable<Key> {
  private final int cpuVCores;
  private final int memoryMb;

  public Key(Entry entry) {
    this.cpuVCores = entry.getCpuVCores();
    this.memoryMb = entry.getMemoryMbs();
  }

  @Override
  public int compareTo(Key o) {
    int comp = memoryMb - o.memoryMb;
    if (comp == 0) {
      comp = cpuVCores - o.cpuVCores;
    }
    return comp;
  }

  @Override
  public int hashCode() {
    return memoryMb + cpuVCores << 16;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj != null && obj instanceof Key) && compareTo((Key) obj) == 0;
  }
}

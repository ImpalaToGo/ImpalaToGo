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
package com.cloudera.llama.am.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.api.Resource.Locality;
import com.cloudera.llama.am.api.TestUtils;

public class TestNormalizedRMResource {
  @Test
  public void testNormalizeOnTheLine() {
    RMResource requested = TestUtils.createRMResource("somenode",
        Locality.MUST, 3, 2048);
    List<NormalizedRMResource> chunks = NormalizedRMResource.normalize(
        requested, 1, 1024);
    assertEquals(5, chunks.size());
    verifyChunks(chunks, 2048, 3, 1024, 1);
  }

  @Test
  public void testNormalizeOffTheLine() {
    RMResource requested = TestUtils.createRMResource("somenode",
        Locality.MUST, 3, 3000);
    List<NormalizedRMResource> chunks = NormalizedRMResource.normalize(
        requested, 1, 1024);
    assertEquals(6, chunks.size());
    verifyChunks(chunks, 3072, 3, 1024, 1);
  }

  @Test
  public void testNormalizeNonMustLocality() {
    RMResource requested = TestUtils.createRMResource("somenode",
        Locality.PREFERRED, 3, 2048);
    List<NormalizedRMResource> chunks = NormalizedRMResource.normalize(
        requested, 1, 1024);
    assertEquals(5, chunks.size());
    verifyChunks(chunks, 2048, 3, 1024, 1);
  }

  private void verifyChunks(List<NormalizedRMResource> chunks,
      int expectedMemSum, int expectedCpuSum, int memSize, int cpuSize) {
    int memSum = 0;
    int cpuSum = 0;
    for (NormalizedRMResource chunk : chunks) {
      assertTrue("Invalid chunk, vcores=" + chunk.getCpuVCoresAsk() + ", mb=" +
          chunk.getMemoryMbsAsk(),
          (chunk.getCpuVCoresAsk() == cpuSize) !=
          (chunk.getMemoryMbsAsk() == memSize));
      cpuSum += chunk.getCpuVCoresAsk();
      memSum += chunk.getMemoryMbsAsk();
      assertEquals(Locality.MUST, chunk.getLocalityAsk());
    }
    assertEquals(expectedMemSum, memSum);
    assertEquals(expectedCpuSum, cpuSum);
  }
}

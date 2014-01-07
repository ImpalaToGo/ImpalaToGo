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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.llama.am.api.LlamaAM;
import com.cloudera.llama.am.api.PlacedResource.Status;
import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.api.TestUtils;
import com.cloudera.llama.am.api.Resource.Locality;
import com.cloudera.llama.am.spi.RMEvent;
import com.cloudera.llama.am.spi.RMListener;
import com.cloudera.llama.util.LlamaException;

public class TestNormalizerRMConnector {

  private RecordingMockRMConnector connector;
  private NormalizerRMConnector normalizer;
  private CallbackStorer listener;

  @Before
  public void setUp() {
    connector = new RecordingMockRMConnector();
    Configuration conf = new Configuration();
    conf.setInt(LlamaAM.NORMALIZING_STANDARD_MBS_KEY, 512);
    conf.setInt(LlamaAM.NORMALIZING_STANDARD_VCORES_KEY, 2);
    normalizer = new NormalizerRMConnector(conf, connector);
    listener = new CallbackStorer();
    normalizer.setLlamaAMCallback(listener);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleReserveAndAllocate() throws LlamaException {
    RMResource request = TestUtils.createRMResource("node1", Locality.MUST, 3,
        3000);
    normalizer.reserve(Arrays.asList(request));
    assertEquals("reserve", connector.invoked.get(1));
    List<RMResource> normalResources = (List<RMResource>) connector.args.get(1);
    assertEquals(2 + 6, normalResources.size());

    for (RMResource normalResource : normalResources) {
      // Normalizer shouldn't call back until it has the full reservation
      assertTrue(listener.events == null || listener.events.size() == 0);
      RMEvent allocateEvent = createAllocationEvent(normalResource);
      normalizer.onEvent(Arrays.asList(allocateEvent));
    }

    assertNotNull(listener.events);
    assertEquals(1, listener.events.size());
    RMEvent event = listener.events.get(0);
    assertEquals(4, event.getCpuVCores());
    assertEquals(512 * 6, event.getMemoryMbs());
    assertEquals("node1", event.getLocation());
    assertEquals(request.getResourceId(), event.getResourceId());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAllocationGreaterThanNormalSize() throws LlamaException {
    RMResource request = TestUtils.createRMResource("node1", Locality.MUST, 3,
        3000);
    normalizer.reserve(Arrays.asList(request));
    assertEquals("reserve", connector.invoked.get(1));
    List<RMResource> normalResources = (List<RMResource>) connector.args.get(1);
    assertEquals(2 + 6, normalResources.size());

    for (RMResource normalResource : normalResources) {
      // Normalizer shouldn't call back until it has the full reservation
      assertTrue(listener.events == null || listener.events.size() == 0);
      RMEvent allocateEvent = createAllocationEvent(normalResource, 3000, 3);
      normalizer.onEvent(Arrays.asList(allocateEvent));
    }

    assertNotNull(listener.events);
    assertEquals(1, listener.events.size());
    RMEvent event = listener.events.get(0);
    assertEquals(24, event.getCpuVCores());
    assertEquals(24000, event.getMemoryMbs());
    assertEquals("node1", event.getLocation());
    assertEquals(request.getResourceId(), event.getResourceId());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRejected() throws Exception {
    RMResource request = TestUtils.createRMResource("node1", Locality.MUST, 3,
        3000);

    normalizer.reserve(Arrays.asList(request));
    assertEquals("reserve", connector.invoked.get(1));
    List<RMResource> normalResources = (List<RMResource>) connector.args.get(1);
    assertEquals(2 + 6, normalResources.size());

    RMEvent rejectionEvent = RMEvent.createStatusChangeEvent(
        normalResources.get(0).getResourceId(), Status.REJECTED);
    normalizer.onEvent(Arrays.asList(rejectionEvent));

    assertNotNull(listener.events);
    assertEquals(1, listener.events.size());
    RMEvent event = listener.events.get(0);
    assertEquals(request.getResourceId(), event.getResourceId());
    assertEquals(Status.REJECTED, event.getStatus());

    // The entry should be removed, both for the original ID and for other
    // normalized IDs from the same original
    assertNull(normalizer.getEntryUsingOriginalId(request.getResourceId()));
    assertNull(normalizer.getEntryUsingNormalizedId(
        normalResources.get(1).getResourceId()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRelease() throws LlamaException {
    RMResource request = TestUtils.createRMResource("node1", Locality.MUST, 3,
        3000);
    normalizer.reserve(Arrays.asList(request));
    normalizer.release(Arrays.asList(request), true);
    assertEquals("release", connector.invoked.get(2));
    List<RMResource> normalResources = (List<RMResource>) connector.args.get(1);
    assertEquals(2 + 6, normalResources.size());
    for (RMResource normalResource : normalResources) {
      assertTrue(!normalResource.getResourceId().equals(request.getResourceId()));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReleaseAfterSomeAllocations() throws LlamaException {
    RMResource request = TestUtils.createRMResource("node1", Locality.MUST, 3,
        3000);
    normalizer.reserve(Arrays.asList(request));
    List<RMResource> reservedResources = (List<RMResource>) connector.args.get(1);
    assertEquals(2 + 6, reservedResources.size());
    for (int i = 0; i < reservedResources.size() - 4; i++) {
      RMEvent allocateEvent = createAllocationEvent(reservedResources.get(i));
      normalizer.onEvent(Arrays.asList(allocateEvent));
    }

    normalizer.release(Arrays.asList(request), true);
    assertEquals("release", connector.invoked.get(2));
    List<RMResource> releasedResources = (List<RMResource>) connector.args.get(1);
    assertEquals(2 + 6, releasedResources.size());
    for (RMResource normalResource : releasedResources) {
      assertTrue(!normalResource.getResourceId().equals(request.getResourceId()));
    }
  }

  /**
   * Creates an allocation event that satisfies all requested capabilities of a
   * resource.
   */
  private RMEvent createAllocationEvent(RMResource resource) {
    return RMEvent.createAllocationEvent(resource.getResourceId(),
        resource.getLocationAsk(), resource.getCpuVCoresAsk(),
        resource.getMemoryMbsAsk(), resource.getRmResourceId(), null);
  }

  private RMEvent createAllocationEvent(RMResource resource, int memoryAlloc,
      int cpuAlloc) {
    return RMEvent.createAllocationEvent(resource.getResourceId(),
        resource.getLocationAsk(), cpuAlloc, memoryAlloc,
        resource.getRmResourceId(), null);
  }

  private class CallbackStorer implements RMListener {
    List<RMEvent> events;

    @Override
    public void stoppedByRM() {
    }

    @Override
    public void onEvent(List<RMEvent> events) {
      this.events = events;
    }
  }
}

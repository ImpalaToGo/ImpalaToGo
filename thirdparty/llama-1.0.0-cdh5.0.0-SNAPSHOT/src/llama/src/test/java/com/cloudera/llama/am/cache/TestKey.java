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

import junit.framework.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestKey {

  @Test
  public void testKey() {
    Entry entry = Mockito.mock(Entry.class);
    Mockito.when(entry.getMemoryMbs()).thenReturn(1024);
    Mockito.when(entry.getCpuVCores()).thenReturn(1);
    Key k1 = new Key(entry);
    Key ka = new Key(entry);
    Mockito.when(entry.getMemoryMbs()).thenReturn(1024);
    Mockito.when(entry.getCpuVCores()).thenReturn(2);
    Key k2 = new Key(entry);
    Mockito.when(entry.getMemoryMbs()).thenReturn(2048);
    Mockito.when(entry.getCpuVCores()).thenReturn(1);
    Key k3 = new Key(entry);
    Assert.assertTrue(k1.compareTo(k1) == 0);
    Assert.assertTrue(k1.compareTo(k2) < 0);
    Assert.assertTrue(k1.compareTo(k3) < 0);
    Assert.assertTrue(k2.compareTo(k1) > 0);
    Assert.assertTrue(k3.compareTo(k1) > 0);
    Assert.assertTrue(k2.compareTo(k3) < 0);
    Assert.assertTrue(k3.compareTo(k2) > 0);
    Assert.assertTrue(k1.equals(k1));
    Assert.assertTrue(k1.equals(ka));
    Assert.assertFalse(k1.equals(k3));
    Assert.assertFalse(k1.equals(k2));
    Assert.assertFalse(k1.equals(null));
    Assert.assertFalse(k1.equals(new Object()));
    Assert.assertTrue(k1.hashCode() == k1.hashCode());
    Assert.assertTrue(k1.hashCode() == ka.hashCode());
    Assert.assertFalse(k1.hashCode() == k3.hashCode());
    Assert.assertFalse(k1.hashCode() == k2.hashCode());
  }

}

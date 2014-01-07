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
package com.cloudera.llama.util;

import junit.framework.Assert;
import org.junit.Test;

public class TestUUID {

  //Java:  0f5dc366968d463394a29c8a3bde83d1
  //Impala: f5dc366968d4633:94a29c8a3bde83d1

  @Test
  public void testUUID() throws Exception {
    java.util.UUID uuid = java.util.UUID.randomUUID();
    UUID uuid1 = new UUID(uuid);
    UUID uuid2 = new UUID(uuid);
    Assert.assertEquals(uuid1, uuid2);
    String s1 = uuid1.toString();
    UUID uuid3 = UUID.fromString(s1);
    Assert.assertEquals(uuid1, uuid3);
  }

  @Test
  public void testImpalaUUIDFormat() throws Exception {
    String impalaValue = "f5dc366968d4633:94a29c8a3bde83d1";
    UUID uuid = UUID.fromString(impalaValue);
    Assert.assertEquals(impalaValue, uuid.toString());
  }

  @Test
  public void testTrimming() throws Exception {
    String str = "0:0";
    UUID uuid = new UUID(0,0);
    Assert.assertEquals(str, uuid.toString());
    Assert.assertEquals(uuid, UUID.fromString(str));

    str = "1f5dc366968d4633:94a29c8a3bde83d1";
    uuid = UUID.fromString(str);
    Assert.assertEquals(str, uuid.toString());

    str = "4633:83d1";
    uuid = UUID.fromString(str);
    Assert.assertEquals(str, uuid.toString());

  }
}

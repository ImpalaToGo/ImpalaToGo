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

public class TestLlamaException {

  @Test
  public void testException1() {
    LlamaException ex = new LlamaException(ErrorCode.TEST);
    Assert.assertEquals(ErrorCode.TEST.getCode(), ex.getErrorCode());
    Assert.assertNull(ex.getCause());
    Assert.assertTrue(ex.getMessage().startsWith(ErrorCode.TEST.toString()));
  }

  @Test
  public void testException2() {
    LlamaException ex = new LlamaException(ErrorCode.TEST, "a", "b");
    Assert.assertEquals(ErrorCode.TEST.getCode(), ex.getErrorCode());
    Assert.assertNull(ex.getCause());
    Assert.assertTrue(ex.getMessage().startsWith(ErrorCode.TEST.toString()));
    Assert.assertTrue(ex.getMessage().contains("a:b"));
  }

  @Test
  public void testException3() {
    Throwable t = new Throwable();
    LlamaException ex = new LlamaException(t, ErrorCode.TEST);
    Assert.assertEquals(ErrorCode.TEST.getCode(), ex.getErrorCode());
    Assert.assertEquals(t, ex.getCause());
    Assert.assertTrue(ex.getMessage().startsWith(ErrorCode.TEST.toString()));
    Assert.assertTrue(ex.getMessage().endsWith(t.toString()));
  }


  @Test
  public void testException4() {
    Throwable t = new Throwable();
    LlamaException ex = new LlamaException(t, ErrorCode.TEST, "a", "b");
    Assert.assertEquals(ErrorCode.TEST.getCode(), ex.getErrorCode());
    Assert.assertEquals(t, ex.getCause());
    Assert.assertTrue(ex.getMessage().startsWith(ErrorCode.TEST.toString()));
    Assert.assertTrue(ex.getMessage().endsWith(t.toString()));
    Assert.assertTrue(ex.getMessage().contains("a:b"));
  }

}

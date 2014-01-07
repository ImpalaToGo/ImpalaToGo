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

import com.cloudera.llama.util.ExceptionUtils;
import junit.framework.Assert;
import org.junit.Test;

public class TestExceptionUtils {

  @Test(expected = IllegalArgumentException.class)
  public void testNullEx() {
    Assert.assertNull(ExceptionUtils.getRootCause(null, null));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullClass() {
    Assert.assertNull(ExceptionUtils.getRootCause(new Exception(), null));
  }

  @Test
  public void testNoRoot() {
    Exception ex = new Exception();
    Assert.assertEquals(ex, ExceptionUtils.getRootCause(ex, RuntimeException
        .class));
  }

  @Test
  public void testExIsRoot() {
    Exception ex = new Exception();
    Assert.assertEquals(ex, ExceptionUtils.getRootCause(ex, Exception.class));
  }

  @Test
  public void testCauseIsRoot() {
    RuntimeException rex = new RuntimeException();
    Exception ex = new Exception(rex);
    Assert.assertEquals(rex, ExceptionUtils.getRootCause(ex,
        RuntimeException.class));
  }

}

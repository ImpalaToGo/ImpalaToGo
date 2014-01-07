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

import com.cloudera.llama.util.ParamChecker;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestParamChecker {

  @Test
  public void testNotNullOK() {
    Object o = new Object();
    Assert.assertSame(o, ParamChecker.notNull(o, "o"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotNullFail() {
    ParamChecker.notNull(null, "o");
  }

  @Test
  public void testNotEmptyOK() {
    String s = "a";
    Assert.assertSame(s, ParamChecker.notNull(s, "s"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotEmptyFail1() {
    ParamChecker.notEmpty(null, "s");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotEmptyFail2() {
    ParamChecker.notEmpty("", "s");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotEmptyFail3() {
    ParamChecker.notEmpty(" ", "s");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNotNullsOK() {
    List l = new ArrayList();
    Assert.assertSame(l, ParamChecker.notNulls(l, "l"));
    l.add(new Object());
    Assert.assertSame(l, ParamChecker.notNulls(l, "l"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotNullsFail1() {
    ParamChecker.notNulls(null, "l");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void testNotNullsFail2() {
    List l = new ArrayList();
    l.add(null);
    ParamChecker.notNulls(l, "l");
  }

  @Test
  public void testGreaterEqualZeroOK() {
    Assert.assertEquals(0, ParamChecker.greaterEqualZero(0, "i"));
    Assert.assertEquals(1, ParamChecker.greaterEqualZero(1, "i"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGreaterEqualZeroFail() {
    ParamChecker.greaterEqualZero(-1, "i");
  }

  @Test
  public void testAssertsOK() {
    ParamChecker.asserts(true, "t");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAssertsFail() {
    ParamChecker.asserts(false, "f");
  }

}

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

import java.util.List;

public class ParamChecker {

  public static <T> T notNull(T t, String paramName) {
    if (t == null) {
      throw new IllegalArgumentException(paramName + " cannot be NULL");
    }
    return t;
  }

  public static String notEmpty(String s, String paramName) {
    notNull(s, paramName);
    if (s.trim().isEmpty()) {
      throw new IllegalArgumentException(paramName + " cannot be empty");
    }
    return s;
  }

  public static <T> List<T> notNulls(List<T> l, String paramName) {
    notNull(l, paramName);
    for (T t : l) {
      if (t == null) {
        throw new IllegalArgumentException(paramName +
            " cannot have NULL elements");
      }
    }
    return l;
  }

  public static int greaterEqualZero(int i, String paramName) {
    return greaterThan(i, -1, paramName);
  }

  public static int greaterThan(int i, int base, String paramName) {
    if (i <= base) {
      throw new IllegalArgumentException(paramName + " must be greater than " + 
          base);
    }
    return i;
  }

  public static void asserts(boolean b, String msg) {
    if (!b) {
      throw new IllegalArgumentException(msg);
    }
  }

}

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

import java.math.BigInteger;

public class UUID {
  private long low;
  private long high;

  UUID(java.util.UUID uuid) {
    this(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
  }

  public UUID(long mostSigBits, long leastSigBits) {
    this.low = leastSigBits;
    this.high = mostSigBits;
  }

  public static UUID randomUUID() {
    return new UUID(java.util.UUID.randomUUID());
  }

  public long getLeastSignificantBits() {
    return low;
  }

  public long getMostSignificantBits() {
    return high;
  }

  @Override
  public int hashCode() {
    long highLow = high ^ low;
    return ((int) (highLow >> 32)) ^ (int) highLow;
  }

  @Override
  public boolean equals(Object obj) {
    boolean ret = false;
    if (obj instanceof UUID) {
      UUID other = (UUID) obj;
      ret = low == other.low && high == other.high;
    }
    return ret;
  }

  public static UUID fromString(String value) {
    ParamChecker.notEmpty(value, "value");
    int sep = value.indexOf(":");
    if (sep == -1) {
      throw new IllegalArgumentException(
          "Invalid UUID string value, missing ':' : " + value);
    }
    String sLow = value.substring(0, sep);
    String sHigh = value.substring(sep + 1);
    if (sLow.length() > 16) {
      throw new IllegalArgumentException(
          "Invalid UUID string value, low is not a 32 bit hexa: " + value);
    }
    if (sHigh.length() > 16) {
      throw new IllegalArgumentException(
          "Invalid UUID string value, high is not a 32 bit hexa: " + value);
    }
    long low = new BigInteger(sLow, 16).longValue();
    long high = new BigInteger(sHigh, 16).longValue();
    return new UUID(high, low);
  }


  @Override
  public String toString() {
    String sLow = Long.toHexString(low);
    String sHigh = Long.toHexString(high);
    return trimZeros(sLow) + ":" + trimZeros(sHigh);
  }

  private String trimZeros(String value) {
    int i = 0;
    int len = value.length() - 1; // we need keep at least one zero if all zeros
    while (i < len && value.charAt(i) == '0') {
      i++;
    }
    return value.substring(i);
  }

}

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

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public abstract class DelayedRunnable implements Delayed, Runnable {
  private long delay;

  public DelayedRunnable(long delay) {
    setDelay(delay);
  }

  public void setDelay(long delay) {
    this.delay = Clock.currentTimeMillis() + delay;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(delay - Clock.currentTimeMillis(),
        TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed o) {
    DelayedRunnable other = (DelayedRunnable) o;
    return (int) (delay - (other.delay));
  }

}

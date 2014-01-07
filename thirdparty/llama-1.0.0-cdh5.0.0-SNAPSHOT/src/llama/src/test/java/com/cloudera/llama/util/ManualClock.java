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

public class ManualClock implements Clock.Impl {
  private volatile long time;

  public ManualClock() {
    this(System.currentTimeMillis());
  }

  public ManualClock(long epoc) {
    time = epoc;
  }

  public synchronized void set(long epoc) {
    time = epoc;
    this.notifyAll();
  }

  public synchronized void increment(long millis) {
    time += millis;
    this.notifyAll();
  }

  @Override
  public synchronized long currentTimeMillis() {
    return time;
  }

  @Override
  public void sleep(long millis) throws InterruptedException {
    long wakeUp = currentTimeMillis() + millis;
    while (currentTimeMillis()- wakeUp < 0) {
      synchronized (this) {
        wait();
      }
    }
  }
}

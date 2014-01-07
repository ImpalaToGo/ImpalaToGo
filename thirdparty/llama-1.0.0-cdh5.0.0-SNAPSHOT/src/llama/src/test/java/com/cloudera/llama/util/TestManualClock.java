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

import java.util.concurrent.CountDownLatch;

public class TestManualClock {

  @Test
  public void testCurrentTimeMillis() {
    try {
      ManualClock manualClock = new ManualClock();
      long time = manualClock.currentTimeMillis();
      Clock.setClock(manualClock);
      Assert.assertEquals(time, Clock.currentTimeMillis());
      manualClock.set(1000);
      Assert.assertEquals(1000, Clock.currentTimeMillis());
      manualClock.increment(1000);
      Assert.assertEquals(2000, Clock.currentTimeMillis());

      manualClock = new ManualClock(1);
      Clock.setClock(manualClock);
      Assert.assertEquals(1, Clock.currentTimeMillis());
    } finally {
      Clock.setClock(Clock.SYSTEM);
    }
  }

  @Test
  public void testSleep() throws Exception {
    try {
      final ManualClock manualClock = new ManualClock(0);
      Clock.setClock(manualClock);
      Clock.sleep(0);
      final CountDownLatch latch = new CountDownLatch(1);
      new Thread() {
        @Override
        public void run() {
          try {
            latch.await();
            Thread.sleep(300);
          } catch (InterruptedException ex) {
            //NOP
          }
          manualClock.increment(10000);
        }
      }.start();
      latch.countDown();
      long start = System.currentTimeMillis();
      Clock.sleep(10000);
      long end = System.currentTimeMillis();
      Assert.assertTrue((end - start) >= 300);
      Assert.assertTrue((end - start) < 500);
    } finally {
      Clock.setClock(Clock.SYSTEM);
    }
  }

}

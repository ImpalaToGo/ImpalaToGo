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
package com.cloudera.llama.server;

import junit.framework.Assert;
import org.apache.thrift.TProcessor;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public class TestThriftServer {
  
  private static class MyRunnable implements Runnable {
    private CountDownLatch inLatch;
    private boolean run;
    
    public MyRunnable(CountDownLatch inLatch) {
      this.inLatch = inLatch;
    }
    
    @Override
    public void run() {
      try {
        inLatch.await();
        run = true;
      } catch (InterruptedException ex) {
        //NOP
      }
    }
    
    public boolean hasRun() {
      return run;
    }
  }
  
  @Test
  public void testMaxingOutThreadPool() throws Exception {
    testThreadPool(false);
  }

  @Test
  public void testOverflowingThreadPool() throws Exception {
    testThreadPool(true);
  }

  private void testThreadPool(boolean exceedQueue) throws Exception {
    ThriftServer ts = new ThriftServer("test", null) {
      @Override
      protected TProcessor createServiceProcessor() {
        return null;
      }

      @Override
      protected void startService() {
      }

      @Override
      protected void stopService() {
      }
    };
    ExecutorService ex = ts.createExecutorService("test", 1, 2);
    try {
      int extra = (exceedQueue) ? 1 : 0;
      int expectedToRun = 2;
      CountDownLatch inLatch = new CountDownLatch(1);
      MyRunnable[] runnables = new MyRunnable[2 + extra];
      for (int i = 0; i < runnables.length; i++) {
        runnables[i] = new MyRunnable(inLatch);
        ex.execute(runnables[i]);
      }
      inLatch.countDown();
      Thread.sleep(300);
      int ran = 0;
      for (int i = 0; i < runnables.length; i++) {
        ran += (runnables[i].hasRun()) ? 1 : 0;
      }
      Assert.assertEquals(expectedToRun, ran);
    } finally {
      ex.shutdownNow();
    }
  }
}

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
package com.cloudera.llama.am.api;

import com.cloudera.llama.am.impl.LlamaAMEventImpl;
import com.cloudera.llama.server.MetricUtil;
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.ParamChecker;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class AsyncLlamaAMListener implements LlamaAMListener {

  private final static String QUEUE_GAUGE = LlamaAM.METRIC_PREFIX +
      ".async.listener.queue.size.gauge";

  private static final int DISPATCH_INTERVAL_MS = 50;

  private static final int DISPATCH_BATCH_SIZE = 500;

  private final LlamaAMListener listener;
  private final BlockingQueue<LlamaAMEvent> changes;
  private final Thread processorThread;
  private volatile boolean running;

  public AsyncLlamaAMListener(LlamaAMListener listener) {
    this.listener = ParamChecker.notNull(listener, "listener");
    changes = new LinkedBlockingQueue<LlamaAMEvent>();
    processorThread = new Thread(new AsyncDispatcher(),
        "llama-am-async-listener");
    processorThread.setDaemon(true);
  }

  public void setMetricRegistry(MetricRegistry metricRegistry) {
    if (metricRegistry != null) {
      MetricUtil.registerGauge(metricRegistry, QUEUE_GAUGE,
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return changes.size();
            }
          });
    }
  }

  public void start() {
    if (running) {
      throw new IllegalStateException("Already started");
    }
    running = true;
    processorThread.start();
  }

  public void stop() {
    running = false;
    processorThread.interrupt();
  }

  @Override
  public void onEvent(LlamaAMEvent event) {
    changes.add(event);
  }

  private class AsyncDispatcher implements Runnable {
    @Override
    public void run() {
      try {
        List<LlamaAMEvent> list = new ArrayList<LlamaAMEvent>();
        while (running) {
          Clock.sleep(DISPATCH_INTERVAL_MS);
          if (changes.peek() != null) {
            changes.drainTo(list, DISPATCH_BATCH_SIZE);
            while (!list.isEmpty()) {
              listener.onEvent(LlamaAMEventImpl.merge(list));
              list.clear();
              changes.drainTo(list, DISPATCH_BATCH_SIZE);
            }
          }
        }
      } catch (InterruptedException ex) {
        //NOP
      }
    }
  }

}

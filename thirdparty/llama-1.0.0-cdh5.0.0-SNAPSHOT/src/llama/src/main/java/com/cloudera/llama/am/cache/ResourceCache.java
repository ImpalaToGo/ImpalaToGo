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
package com.cloudera.llama.am.cache;

import com.cloudera.llama.am.api.LlamaAM;
import com.cloudera.llama.util.Clock;
import com.cloudera.llama.util.ParamChecker;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ResourceCache extends ResourceStore {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceCache.class);

  public interface EvictionPolicy {
    public boolean shouldEvict(CacheRMResource resource);
  }

  public interface Listener {
    public void onEviction(CacheRMResource cachedRMResource);
  }

  public static final String PREFIX = LlamaAM.PREFIX_KEY + "cache.";

  public static final String EVICTION_POLICY_CLASS_KEY =
      PREFIX + "eviction.policy.class";

  public static final String EVICTION_RUN_INTERVAL_KEY =
      PREFIX + "eviction.run.interval.timeout.ms";

  public static final int EVICTION_RUN_INTERVAL_DEFAULT = 5000;

  public static final String EVICTION_IDLE_TIMEOUT_KEY =
      PREFIX + "eviction.timeout.policy.idle.timeout.ms";

  public static final int EVICTION_IDLE_TIMEOUT_DEFAULT = 30000;

  public static class TimeoutEvictionPolicy
      implements EvictionPolicy, Configurable {

    private Configuration conf;
    private long timeout;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      timeout = conf.getInt(EVICTION_IDLE_TIMEOUT_KEY,
          EVICTION_IDLE_TIMEOUT_DEFAULT);
    }

    public long getTimeout() {
      return timeout;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public boolean shouldEvict(CacheRMResource resource) {
      return ((Clock.currentTimeMillis() - resource.getCachedOn()) - timeout)
          >= 0;
    }
  }

  private final String queue;
  private final EvictionPolicy evictionPolicy;
  private volatile boolean running;
  private final int evictionRunInterval;
  private Thread evictionThread;
  private final Listener listener;

  public ResourceCache(String queue, Configuration conf, Listener listener) {
    this.queue = ParamChecker.notEmpty(queue, "queue");
    this.listener = ParamChecker.notNull(listener, "listener");
    Class<? extends EvictionPolicy> klass =
        conf.getClass(EVICTION_POLICY_CLASS_KEY, TimeoutEvictionPolicy.class,
            EvictionPolicy.class);
    evictionPolicy = ReflectionUtils.newInstance(klass, conf);
    evictionRunInterval = conf.getInt(EVICTION_RUN_INTERVAL_KEY,
        EVICTION_RUN_INTERVAL_DEFAULT);
  }


  public synchronized void start() {
    if (running) {
      throw new IllegalStateException("Already started");
    }
    LOG.debug("EvictionPolicy '{}'", evictionPolicy.getClass().getSimpleName());
    LOG.debug("Eviction run interval '{}'ms", evictionRunInterval);
    running = true;
    evictionThread = new Thread("llama-resource-cache-eviction:" + queue) {
      @Override
      public void run() {
        while (running) {
          try {
            Clock.sleep(evictionRunInterval);
          } catch (InterruptedException ex) {
            //NOP
          }
          if (running) {
            runEviction();
          }
        }
      }
    };
    evictionThread.setDaemon(true);
    evictionThread.start();
  }

  void runEviction() {
    LOG.trace("Running eviction for '{}'", queue);
    List<Entry> entries = getEntries();
    if (!entries.isEmpty()) {
      LOG.debug("Eviction processing '{}' entries", entries.size());
    }
    int counter = 0;
    for (Entry entry : entries) {
      if (entry.isValid() && evictionPolicy.shouldEvict(entry)) {
        if (findAndRemove(entry.getResourceId()) != null) {
          try {
            listener.onEviction(entry);
          } catch (Throwable ex) {
            LOG.error("Listener error processing eviction for '{}', {}",
                entry.getRmResourceId(), ex.toString(), ex);
          }
          LOG.debug("Evicted '{}' from queue '{}'",
              entry.getRmResourceId(), queue);
          counter++;
        }
      }
    }
    if (counter > 0) {
      LOG.debug("Eviction run, evicted '{}' entries", counter);
    }
  }

  public synchronized void stop() {
    running = false;
    evictionThread.interrupt();
    try {
      evictionThread.join();
    } catch (InterruptedException ex) {
      //NOP
    }
  }


}

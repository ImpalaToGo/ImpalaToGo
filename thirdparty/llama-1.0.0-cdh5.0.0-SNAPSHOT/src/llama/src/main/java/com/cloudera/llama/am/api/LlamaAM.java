/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional inforAMtion
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you AMy not use this file except in compliance
 * with the License.  You AMy obtain a copy of the License at
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

import com.cloudera.llama.am.impl.APIContractLlamaAM;
import com.cloudera.llama.am.impl.ExpansionReservationsLlamaAM;
import com.cloudera.llama.am.impl.GangAntiDeadlockLlamaAM;
import com.cloudera.llama.am.impl.MultiQueueLlamaAM;
import com.cloudera.llama.util.LlamaException;
import com.cloudera.llama.util.ParamChecker;
import com.cloudera.llama.util.UUID;
import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public abstract class LlamaAM {
  public static final String PREFIX_KEY = "llama.am.";

  public static final String METRIC_PREFIX = "llama.am.";

  public static final String RM_CONNECTOR_CLASS_KEY = PREFIX_KEY +
      "rm.connector.class";

  public static final String CORE_QUEUES_KEY = PREFIX_KEY +
      "core.queues";

  public static final String GANG_ANTI_DEADLOCK_ENABLED_KEY = PREFIX_KEY +
      "gang.anti.deadlock.enabled";
  public static final boolean GANG_ANTI_DEADLOCK_ENABLED_DEFAULT = true;

  public static final String GANG_ANTI_DEADLOCK_NO_ALLOCATION_LIMIT_KEY =
      PREFIX_KEY + "gang.anti.deadlock.no.allocation.limit.ms";
  public static final long GANG_ANTI_DEADLOCK_NO_ALLOCATION_LIMIT_DEFAULT =
      30000;

  public static final String GANG_ANTI_DEADLOCK_BACKOFF_PERCENT_KEY =
      PREFIX_KEY + "gang.anti.deadlock.backoff.percent";
  public static final int GANG_ANTI_DEADLOCK_BACKOFF_PERCENT_DEFAULT =
      30;

  public static final String GANG_ANTI_DEADLOCK_BACKOFF_MIN_DELAY_KEY =
      PREFIX_KEY + "gang.anti.deadlock.backoff.min.delay.ms";
  public static final long GANG_ANTI_DEADLOCK_BACKOFF_MIN_DELAY_DEFAULT = 10000;

  public static final String GANG_ANTI_DEADLOCK_BACKOFF_MAX_DELAY_KEY =
      PREFIX_KEY + "gang.anti.deadlock.backoff.max.delay.ms";
  public static final long GANG_ANTI_DEADLOCK_BACKOFF_MAX_DELAY_DEFAULT = 30000;

  public static final String CACHING_ENABLED_KEY =
      PREFIX_KEY + "caching.enabled";
  public static final boolean CACHING_ENABLED_DEFAULT = true;

  public static final String THROTTLING_ENABLED_KEY =
      PREFIX_KEY + "throttling.enabled";
  public static final boolean THROTTLING_ENABLED_DEFAULT = true;
  
  public static final String QUEUE_AM_EXPIRE_MS =
      PREFIX_KEY + "queue.expire.ms";
  public static final int QUEUE_AM_EXPIRE_MS_DEFAULT = 5 * 60 * 1000;

  public static final String NORMALIZING_ENABLED_KEY =
      PREFIX_KEY + "resource.normalizing.enabled";
  public static final boolean NORMALIZING_ENABLED_DEFAULT = true;

  public static final String NORMALIZING_STANDARD_MBS_KEY =
      PREFIX_KEY + "resource.normalizing.standard.mbs";
  public static final int NORMALIZING_SIZE_MBS_DEFAULT = 1024;

  public static final String NORMALIZING_STANDARD_VCORES_KEY =
      PREFIX_KEY + "resource.normalizing.standard.vcores";
  public static final int NORMALIZING_SIZE_VCORES_DEFAULT = 1;

  private static Configuration cloneConfiguration(Configuration conf) {
    Configuration clone = new Configuration(false);
    for (Map.Entry<String, String> entry : conf) {
      clone.set(entry.getKey(), entry.getValue());
    }
    return clone;
  }

  public static LlamaAM create(Configuration conf)
      throws LlamaException {
    conf = cloneConfiguration(conf);
    LlamaAM am = new MultiQueueLlamaAM(conf);
    if (conf.getBoolean(GANG_ANTI_DEADLOCK_ENABLED_KEY,
        GANG_ANTI_DEADLOCK_ENABLED_DEFAULT)) {
      am = new GangAntiDeadlockLlamaAM(conf, am);
    }
    am = new ExpansionReservationsLlamaAM(am);
    return new APIContractLlamaAM(am);
  }

  private MetricRegistry metricRegistry;
  private Configuration conf;

  protected LlamaAM(Configuration conf) {
    this.conf = ParamChecker.notNull(conf, "conf");
  }

  public void setMetricRegistry(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  protected MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public Configuration getConf() {
    return conf;
  }

  public abstract void start() throws LlamaException;

  public abstract void stop();

  public abstract boolean isRunning();

  public abstract List<String> getNodes() throws LlamaException;

  public abstract void reserve(UUID reservationId, Reservation reservation)
      throws LlamaException;

  public UUID reserve(Reservation reservation)
      throws LlamaException {
    UUID id = UUID.randomUUID();
    reserve(id, reservation);
    return id;
  }

  public void expand(UUID expansionId, Expansion expansion)
      throws LlamaException {
    throw new UnsupportedOperationException();
  }

  public UUID expand(Expansion expansion)
      throws LlamaException {
    UUID id = UUID.randomUUID();
    expand(id, expansion);
    return id;
  }

  public abstract PlacedReservation getReservation(UUID reservationId)
      throws LlamaException;

  public static final UUID WILDCARD_HANDLE = UUID.randomUUID();

  /**
   * If the reservation does not exist, returns null.
   */
  public abstract PlacedReservation releaseReservation(UUID handle,
      UUID reservationId, boolean doNotCache)
      throws LlamaException;

  public abstract List<PlacedReservation> releaseReservationsForHandle(
      UUID handle, boolean doNotCache)
      throws LlamaException;

  public abstract List<PlacedReservation> releaseReservationsForQueue(
      String queue, boolean doNotCache) throws LlamaException;

  public static final String ALL_QUEUES = "ALL QUEUES";

  public abstract void emptyCacheForQueue(String queue) throws LlamaException;

  public abstract void addListener(LlamaAMListener listener);

  public abstract void removeListener(LlamaAMListener listener);

  private static final ThreadLocal<Boolean> AS_ADMIN =
      new ThreadLocal<Boolean>();

  protected boolean isAdminCall() {
    return (AS_ADMIN.get() != null) ? AS_ADMIN.get() : false;
  }

  protected boolean isCallConsideredEcho(UUID handle) {
    return !isAdminCall() && !handle.equals(WILDCARD_HANDLE);
  }

  public static <T> T doAsAdmin(Callable<T> callable) throws Exception {
    AS_ADMIN.set(Boolean.TRUE);
    try{
      return callable.call();
    } finally {
      AS_ADMIN.remove();
    }
  }

}

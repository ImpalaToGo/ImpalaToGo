// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.util;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.google.common.base.Preconditions;

/**
 * Utility class for submitting and dropping HDFS cache requests.
 */
public class HdfsCachingUtil {
  private static final Logger LOG = Logger.getLogger(HdfsCachingUtil.class);

  // The key name used to save cache directive IDs in table/partition properties.
  private final static String CACHE_DIR_ID_PROP_NAME = "cache_directive_id";

  // The number of caching refresh intervals that can go by when waiting for data to
  // become cached before assuming no more progress is being made.
  private final static int MAX_UNCHANGED_CACHING_REFRESH_INTERVALS = 5;

  // Elena: remove dfs currently
  // private final static DistributedFileSystem dfs;
  /*private final static FileSystem dfs;
  static {
    try {
      dfs = FileSystemUtil.getDistributedFileSystem();
    } catch (IOException e) {
      throw new RuntimeException("HdfsCachingUtil failed to initialize the " +
          "DistributedFileSystem: ", e);
    }
  }
*/
  /**
   * Caches the location of the given Hive Metastore Table and updates the
   * table's properties with the submitted cache directive ID.
   * Returns the ID of the submitted cache directive and throws if there is an error
   * submitting the directive or if the table was already cached.
   */
  public static long submitCacheTblDirective(
      org.apache.hadoop.hive.metastore.api.Table table,
      String poolName) throws ImpalaRuntimeException {
    if (table.getParameters().get(CACHE_DIR_ID_PROP_NAME) != null) {
      throw new ImpalaRuntimeException(String.format(
          "Table is already cached: %s.%s", table.getDbName(), table.getTableName()));
    }
    long id = HdfsCachingUtil.submitDirective(new Path(table.getSd().getLocation()),
        poolName);
    table.putToParameters(CACHE_DIR_ID_PROP_NAME, Long.toString(id));
    return id;
  }

  /**
   * Caches the location of the given Hive Metastore Partition and updates the
   * partitions's properties with the submitted cache directive ID.
   * Returns the ID of the submitted cache directive and throws if there is an error
   * submitting the directive.
   */
  public static long submitCachePartitionDirective(
      org.apache.hadoop.hive.metastore.api.Partition part,
      String poolName) throws ImpalaRuntimeException {
    if (part.getParameters().get(CACHE_DIR_ID_PROP_NAME) != null) {
      throw new ImpalaRuntimeException(String.format(
          "Partition is already cached: %s.%s/%s", part.getDbName(), part.getTableName(),
          part.getValues()));
    }
    long id = HdfsCachingUtil.submitDirective(new Path(part.getSd().getLocation()),
        poolName);
    part.putToParameters(CACHE_DIR_ID_PROP_NAME, Long.toString(id));
    return id;
  }

  /**
   * Removes the cache directive associated with the table from HDFS, uncaching all
   * data. Also updates the table's metadata. No-op if the table is not cached.
   */
  public static void uncacheTbl(org.apache.hadoop.hive.metastore.api.Table table)
      throws ImpalaRuntimeException {
    Preconditions.checkNotNull(table);
    LOG.debug("Uncaching table: " + table.getDbName() + "." + table.getTableName());
    Long id = getCacheDirIdFromParams(table.getParameters());
    if (id == null) return;
    HdfsCachingUtil.removeDirective(id);
    table.getParameters().remove(CACHE_DIR_ID_PROP_NAME);
  }

  /**
   * Removes the cache directive associated with the partition from HDFS, uncaching all
   * data. Also updates the partition's metadata to remove the cache directive ID.
   * No-op if the table is not cached.
   */
  public static void uncachePartition(
      org.apache.hadoop.hive.metastore.api.Partition part) throws ImpalaException {
    Preconditions.checkNotNull(part);
    Long id = getCacheDirIdFromParams(part.getParameters());
    if (id == null) return;
    HdfsCachingUtil.removeDirective(id);
    part.getParameters().remove(CACHE_DIR_ID_PROP_NAME);
  }

  /**
   * Returns the cache directive ID from the given table/partition parameter
   * map. Returns null if the CACHE_DIR_ID_PROP_NAME key was not set or if
   * there was an error parsing the associated ID.
   */
  public static Long getCacheDirIdFromParams(Map<String, String> params) {
    // Always return null on CDH4 to indicate the table/partition is not cached.
    return null;
  }

  /**
   * Given a cache directive ID, returns the pool the directive is cached in.
   * Returns null if no outstanding cache directive match this ID.
   */
  public static String getCachePool(long requestId) throws ImpalaRuntimeException {
    throw new UnsupportedOperationException("HDFS caching is not supported on CDH4");
  }

  /**
   * Waits on a cache directive to either complete or stop making progress. Progress is
   * checked by polling the HDFS caching stats every
   * DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS. We verify the request's
   * "currentBytesCached" is increasing compared to "bytesNeeded".
   * If "currentBytesCached" == "bytesNeeded" or if no progress is made for a
   * MAX_UNCHANGED_CACHING_REFRESH_INTERVALS, this function returns.
   */
  public static void waitForDirective(long directiveId)
      throws ImpalaRuntimeException  {
    throw new UnsupportedOperationException("HDFS caching is not supported on CDH4");
  }

  /**
   * Submits a new caching directive for the specified cache pool name and path.
   * Returns the directive ID if the submission was successful or an
   * ImpalaRuntimeException if the submission fails.
   */
  private static long submitDirective(Path path, String poolName)
      throws ImpalaRuntimeException {
    Preconditions.checkNotNull(path);
    Preconditions.checkState(poolName != null && !poolName.isEmpty());
    throw new UnsupportedOperationException("HDFS caching is not supported on CDH4");
  }

  /**
   * Removes the given cache directive if it exists, uncaching the data. If the
   * cache request does not exist in HDFS no error is returned.
   * Throws an ImpalaRuntimeException if there was any problem removing the
   * directive.
   */
  private static void removeDirective(long directiveId) throws ImpalaRuntimeException {
    LOG.debug("Removing cache directive id: " + directiveId);
    throw new UnsupportedOperationException("HDFS caching is not supported on CDH4");
  }
}

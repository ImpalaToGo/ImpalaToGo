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

package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;

/**
 * Utility methods to interact with a job.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class JobUtil {
  private static final Log LOG = LogFactory.getLog(JobUtil.class);

  protected JobUtil() {
    super();
  }

  /**
   * Initializes the staging directory and returns the path.
   * <p>
   * CLOUDERA-SPECIFIC-NOTE:
   * MR1 and MR2 are incompatible regarding getStagingDir() API.
   * <p>
   * The following code is to handle both MR1 and MR2 at
   * compile/run time. This is done using reflection to figure out the right API.
   *
   * @param conf system configuration
   * @return staging directory path
   * @throws IOException
   * @throws InterruptedException
   */
  public static Path getStagingDir(Configuration conf)
      throws IOException, InterruptedException {
    Path stagingDirPath = null;
    // The API to get staging directory is different in MR1 and MR2/YARN. We first try MR1, and if
    // it is not present, fall back to MR2.
    try {
      stagingDirPath = getStagingDirFromMR1(conf);
    } catch (NoSuchMethodException e) {
      stagingDirPath = getStagingDirFromMR2(conf);
    }
    if (stagingDirPath != null) LOG.debug("Staging dir of the job is: " + stagingDirPath);
    return stagingDirPath;
  }

  /**
   * Invokes {@link JobSubmissionFiles#getStagingDir(org.apache.hadoop.mapred.JobClient,
   * Configuration)} API, if present, to get staging dir path.
   * @param conf
   * @return stagingDir path for the job
   * @throws IOException
   * @throws NoSuchMethodException
   */
  private static Path getStagingDirFromMR1(Configuration conf) throws IOException,
      NoSuchMethodException, InterruptedException {
    Path stagingDirPath;
    JobClient jobClient = new JobClient(new JobConf(conf));
    Method getStagingDirMethod = JobSubmissionFiles.class.getMethod("getStagingDir",
      jobClient.getClass(), conf.getClass());
    try {
      // call this mr1 specific call:
      // JobSubmissionFiles.getStagingDir(jobClient, conf);
      stagingDirPath = (Path) getStagingDirMethod.invoke(null, jobClient, conf);
    } catch (IllegalArgumentException iae) {
      throw new IllegalStateException(iae);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (InvocationTargetException ite) {
      throw new IllegalStateException(ite);
    }
    return stagingDirPath;
  }

  /**
   * Invokes {@link JobSubmissionFiles#getStagingDir(org.apache.hadoop.mapreduce.Cluster,
   *  Configuration)} API, if present, to get the staging dir path.
   * @param conf
   * @return stagingDir path for the job
   */
  private static Path getStagingDirFromMR2(Configuration conf) {
    Path stagingDirPath = null;
    try {
      Class<?> clusterClass = Class.forName("org.apache.hadoop.mapreduce.Cluster");
      Method getStagingDirMethod = JobSubmissionFiles.class.getMethod("getStagingDir",
        clusterClass, conf.getClass());
      Constructor<?> ctr = clusterClass.getConstructor(conf.getClass());
      Object clusterInstance = ctr.newInstance(conf);
      // call this mr2 specific call:
      // JobSubmissionFiles.getStagingDir(cluster, conf);
      stagingDirPath = (Path) getStagingDirMethod.invoke(null, clusterInstance, conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalStateException(cnfe);
    } catch (SecurityException se) {
      throw new IllegalStateException(se);
    } catch (NoSuchMethodException nsme) {
      throw new IllegalStateException(nsme);
    } catch (IllegalArgumentException iae) {
      throw new IllegalStateException(iae);
    } catch (InstantiationException ie) {
      throw new IllegalStateException(ie);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (InvocationTargetException ite) {
      throw new IllegalStateException(ite);
    }
    return stagingDirPath;
  }
}

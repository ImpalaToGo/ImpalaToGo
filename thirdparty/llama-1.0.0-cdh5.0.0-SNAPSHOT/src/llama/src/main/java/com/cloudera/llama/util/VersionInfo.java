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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class VersionInfo {
  private static final Logger LOG = LoggerFactory.getLogger(VersionInfo.class);

  private static final String BUILD_INFO_PROPS = "llama-build-info.properties";

  private static final Properties INFO = new Properties();

  static {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    InputStream is = cl.getResourceAsStream(BUILD_INFO_PROPS);
    if (is != null) {
      try {
        INFO.load(is);
        is.close();
      } catch (Exception ex) {
        LOG.warn("Could not read '{}' from classpath: {}", BUILD_INFO_PROPS,
            ex.toString(), ex);
      }
    }
  }

  public static String getVersion() {
    return INFO.getProperty("llama.version", "?");
  }

  public static String getSourceMD5() {
    return INFO.getProperty("llama.source.md5", "?");
  }

  public static String getBuiltDate() {
    return INFO.getProperty("llama.built.date", "?");
  }

  public static String getBuiltBy() {
    return INFO.getProperty("llama.built.by", "?");
  }

  public static String getSCMURI() {
    return INFO.getProperty("llama.scm.uri", "?");
  }

  public static String getSCMRevision() {
    return INFO.getProperty("llama.scm.revision", "?");
  }

  public static String getHadoopVersion() {
    return org.apache.hadoop.util.VersionInfo.getVersion();
  }

}

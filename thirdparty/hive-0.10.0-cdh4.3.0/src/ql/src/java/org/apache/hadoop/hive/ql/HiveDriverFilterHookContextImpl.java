/*
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

package org.apache.hadoop.hive.ql;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

public class HiveDriverFilterHookContextImpl implements HiveDriverFilterHookContext {

  private Configuration conf;
  private final HiveOperation hiveOperation;
  private final String userName;
  private List<String> result;
  private final String dbName;

  public HiveDriverFilterHookContextImpl(Configuration conf, HiveOperation hiveOperation,
    String userName, List<String> result, String dbName) {
    this.conf = conf;
    this.hiveOperation = hiveOperation;
    this.userName = userName;
    this.result = result;
    this.dbName = dbName;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public String getUserName() {
    return userName;
  }

  public List<String> getResult() {
    return result;
  }

  public HiveOperation getHiveOperation() {
    return hiveOperation;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public String getDbName() {
    return dbName;
  }

  public void setResult(List<String> result) {
    this.result = result;
  }

}

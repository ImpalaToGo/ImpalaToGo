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
package org.apache.hive.service.cli.operation;



import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.session.HiveSession;

public abstract class ExecuteStatementOperation extends Operation {
  protected String statement = null;
  protected Map<String, String> confOverlay = new HashMap<String, String>();

  public ExecuteStatementOperation(HiveSession parentSession, String statement, Map<String, String> confOverlay) {
    super(parentSession, OperationType.EXECUTE_STATEMENT);
    this.statement = statement;
    this.confOverlay = confOverlay;
  }

  public String getStatement() {
    return statement;
  }

  public static ExecuteStatementOperation newExecuteStatementOperation(
      HiveSession parentSession, String statement, Map<String, String> confOverlay) throws HiveSQLException {
    String[] tokens = statement.trim().split("\\s+");
    String command = tokens[0].toLowerCase();

    ExecuteStatementOperation newExecOP;
    HiveConf hiveConf = parentSession.getHiveConf();
    boolean allowExternalExec = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHZ_EXTERNAL_EXEC);
    if ("set".equals(command)) {
       newExecOP = new SetOperation(parentSession, statement, confOverlay);
    } else if ("dfs".equals(command)) {
      if (allowExternalExec) {
        newExecOP = new DfsOperation(parentSession, statement, confOverlay);
      } else {
        throw new HiveSQLException("Insufficient privileges to execute dfs", "42000");
      }
    } else if ("add".equals(command)) {
      if (allowExternalExec) {
        newExecOP = new AddResourceOperation(parentSession, statement, confOverlay);
      } else {
        throw new HiveSQLException("Insufficient privileges to execute add", "42000");
      }
    } else if ("delete".equals(command)) {
      if (allowExternalExec) {
        newExecOP = new DeleteResourceOperation(parentSession, statement, confOverlay);
      } else {
        throw new HiveSQLException("Insufficient privileges to execute delete", "42000");
      }

    } else {
      newExecOP = new SQLOperation(parentSession, statement, confOverlay);
      // check if this is needs to be run asynchronously
      boolean isAsyncOP = (parentSession.getHiveConf().getBoolVar(ConfVars.HIVE_SERVER2_BLOCKING_QUERY) == false);
      if(confOverlay.containsKey(ConfVars.HIVE_SERVER2_BLOCKING_QUERY.toString())) {
        isAsyncOP = confOverlay.get(ConfVars.HIVE_SERVER2_BLOCKING_QUERY.toString()).equalsIgnoreCase("false");
      }
      if (isAsyncOP) {
        newExecOP = AsyncExecStmtOperation.wrapExecStmtOperation(newExecOP);
      }
    }

    return newExecOP;
  }
}

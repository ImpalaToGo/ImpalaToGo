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

package org.apache.hive.service.sql.session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.sql.GetInfoType;
import org.apache.hive.service.sql.GetInfoValue;
import org.apache.hive.service.sql.HiveSQLException;
import org.apache.hive.service.sql.OperationHandle;
import org.apache.hive.service.sql.SessionHandle;
import org.apache.hive.service.sql.operation.ExecuteStatementOperation;
import org.apache.hive.service.sql.operation.GetCatalogsOperation;
import org.apache.hive.service.sql.operation.GetColumnsOperation;
import org.apache.hive.service.sql.operation.GetFunctionsOperation;
import org.apache.hive.service.sql.operation.GetSchemasOperation;
import org.apache.hive.service.sql.operation.GetTableTypesOperation;
import org.apache.hive.service.sql.operation.GetTypeInfoOperation;
import org.apache.hive.service.sql.operation.MetadataOperation;
import org.apache.hive.service.sql.operation.OperationManager;

/**
 * Session.
 *
 */
public class Session {

  private final SessionHandle sessionHandle = new SessionHandle();
  private final String username;
  private final String password;
  private final Map<String, String> sessionConf = new HashMap<String, String>();
  private final HiveConf hiveConf = new HiveConf();
  private final SessionState sessionState;

  private static final String FETCH_WORK_SERDE_CLASS =
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

  private SessionManager sessionManager;
  private OperationManager operationManager;
  private IMetaStoreClient metastoreClient = null;

  public Session(String username, String password, Map<String, String> sessionConf) {
    this.username = username;
    this.password = password;

    if (sessionConf != null) {
      sessionConf.putAll(sessionConf);
    }

    sessionState = new SessionState(hiveConf);
  }

  private SessionManager getSessionManager() {
    return sessionManager;
  }

  public void setSessionManager(SessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  private OperationManager getOperationManager() {
    return operationManager;
  }

  public void setOperationManager(OperationManager operationManager) {
    this.operationManager = operationManager;
  }

  private synchronized void acquire() {
    SessionState.start(sessionState);
  }

  private synchronized void release() {
    assert sessionState != null;
    // no need to release sessionState...
  }

  public SessionHandle getSessionHandle() {
    return sessionHandle;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public HiveConf getHiveConf() {
    hiveConf.setVar(HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE, FETCH_WORK_SERDE_CLASS);
    return hiveConf;
  }

  public IMetaStoreClient getMetaStoreClient() throws HiveSQLException {
    if (metastoreClient == null) {
      try {
        metastoreClient = new HiveMetaStoreClient(getHiveConf());
      } catch (MetaException e) {
        throw new HiveSQLException(e);
      }
    }
    return metastoreClient;
  }

  public GetInfoValue getInfo(GetInfoType getInfoType)
      throws HiveSQLException {
    // FIXME
    throw new HiveSQLException("Not implemented!");
  }

  public OperationHandle executeStatement(String statement, Map<String, String> confOverlay)
      throws HiveSQLException {
    acquire();
    try {
      ExecuteStatementOperation operation = getOperationManager()
          .newExecuteStatementOperation(this, statement, confOverlay);
      operation.run();
      return operation.getHandle();
    } finally {
      release();
    }
  }

  public OperationHandle getTypeInfo()
      throws HiveSQLException {
    acquire();
    try {
      GetTypeInfoOperation operation = getOperationManager().newGetTypeInfoOperation(this);
      operation.run();
      return operation.getHandle();
    } finally {
      release();
    }
  }

  public OperationHandle getCatalogs()
      throws HiveSQLException {
    acquire();
    try {
      GetCatalogsOperation operation = getOperationManager().newGetCatalogsOperation(this);
      operation.run();
      return operation.getHandle();
    } finally {
      release();
    }
  }

  public OperationHandle getSchemas(String catalogName, String schemaName)
      throws HiveSQLException {
      acquire();
    try {
      GetSchemasOperation operation =
          getOperationManager().newGetSchemasOperation(this, catalogName, schemaName);
      operation.run();
      return operation.getHandle();
    } finally {
      release();
    }
  }

  public OperationHandle getTables(String catalogName, String schemaName, String tableName,
      List<String> tableTypes)
      throws HiveSQLException {
      acquire();
    try {
      MetadataOperation operation =
          getOperationManager().newGetTablesOperation(this, catalogName, schemaName, tableName, tableTypes);
      operation.run();
      return operation.getHandle();
    } finally {
      release();
    }
  }

  public OperationHandle getTableTypes()
      throws HiveSQLException {
      acquire();
    try {
      GetTableTypesOperation operation = getOperationManager().newGetTableTypesOperation(this);
      operation.run();
      return operation.getHandle();
    } finally {
      release();
    }
  }

  public OperationHandle getColumns(String catalogName, String schemaName,
      String tableName, String columnName)  throws HiveSQLException {
    acquire();
    try {
    GetColumnsOperation operation = getOperationManager().newGetColumnsOperation(this,
        catalogName, schemaName, tableName, columnName);
    operation.run();
    return operation.getHandle();
    } finally {
      release();
    }
  }

  public OperationHandle getFunctions(String catalogName, String schemaName, String functionName)
      throws HiveSQLException {
    acquire();
    try {
      GetFunctionsOperation operation = getOperationManager()
          .newGetFunctionsOperation(this, catalogName, schemaName, functionName);
      operation.run();
      return operation.getHandle();
    } finally {
      release();
    }
  }

  public void close()
      throws HiveSQLException {
    // throw new HiveSQLException("Not implemented!");
    // TODO: add close operation
    return;
  }

  public SessionState getSessionState() {
    return sessionState;
  }

}

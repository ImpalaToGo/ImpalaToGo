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

package org.apache.hive.service.cli.session;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.log.LogManager;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.operation.GetCatalogsOperation;
import org.apache.hive.service.cli.operation.GetColumnsOperation;
import org.apache.hive.service.cli.operation.GetFunctionsOperation;
import org.apache.hive.service.cli.operation.GetSchemasOperation;
import org.apache.hive.service.cli.operation.GetTableTypesOperation;
import org.apache.hive.service.cli.operation.GetTypeInfoOperation;
import org.apache.hive.service.cli.operation.MetadataOperation;
import org.apache.hive.service.cli.operation.OperationManager;

/**
 * HiveSession
 *
 */
public class HiveSessionImpl implements HiveSession {

  private final SessionHandle sessionHandle = new SessionHandle();
  private String username;
  private final String password;
  private final Map<String, String> sessionConf = new HashMap<String, String>();
  private final HiveConf hiveConf = new HiveConf();
  private final SessionState sessionState;

  private static final String FETCH_WORK_SERDE_CLASS =
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
  private static final Log LOG = LogFactory.getLog(HiveSessionImpl.class);


  private SessionManager sessionManager;
  private OperationManager operationManager;
  private LogManager logManager;
  private IMetaStoreClient metastoreClient = null;
  private String ipAddress;

  public HiveSessionImpl(String username, String password, Map<String, String> sessionConf, String ipAddress) {
    this.username = username;
    this.password = password;
    this.ipAddress = ipAddress;

    if (sessionConf != null) {
      for (Map.Entry<String, String> entry : sessionConf.entrySet()) {
        hiveConf.set(entry.getKey(), entry.getValue());
      }
    }
    // set an explicit session name to control the download directory name
    hiveConf.set(ConfVars.HIVESESSIONID.varname,
        sessionHandle.getHandleIdentifier().toString());
    sessionState = new SessionState(hiveConf);
  }

  public SessionManager getSessionManager() {
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

  protected synchronized void acquire() throws HiveSQLException {
    SessionState.start(sessionState);
  }

  protected synchronized void release() {
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

  public LogManager getLogManager() {
    return logManager;
  }

  public void setLogManager(LogManager logManager) {
    this.logManager = logManager;
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
    acquire();
    try {
      switch (getInfoType) {
      case CLI_SERVER_NAME:
        return new GetInfoValue("Hive");
      case CLI_DBMS_NAME:
        return new GetInfoValue("Apache Hive");
      case CLI_DBMS_VER:
        return new GetInfoValue("0.10.0");
      case CLI_MAX_COLUMN_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_MAX_SCHEMA_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_MAX_TABLE_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_TXN_CAPABLE:
      default:
        throw new HiveSQLException("Unrecognized GetInfoType value: "  + getInfoType.toString());
      }
    } finally {
      release();
    }
  }

  public OperationHandle executeStatement(String statement, Map<String, String> confOverlay)
      throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      ExecuteStatementOperation operation = getOperationManager()
          .newExecuteStatementOperation(this, statement, confOverlay);
      //Log capture
      getLogManager().unregisterCurrentThread();
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);

      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      return operationHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getTypeInfo()
      throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      GetTypeInfoOperation operation = getOperationManager().newGetTypeInfoOperation(this);
      getLogManager().unregisterCurrentThread();

      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      return operationHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getCatalogs() throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      GetCatalogsOperation operation = getOperationManager().newGetCatalogsOperation(this);
      getLogManager().unregisterCurrentThread();

      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      return operationHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getSchemas(String catalogName, String schemaName)
      throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      GetSchemasOperation operation =
          getOperationManager().newGetSchemasOperation(this, catalogName, schemaName);
      getLogManager().unregisterCurrentThread();

      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      return operationHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getTables(String catalogName, String schemaName, String tableName,
      List<String> tableTypes) throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      MetadataOperation operation =
        getOperationManager().newGetTablesOperation(this, catalogName, schemaName, tableName, tableTypes);
      getLogManager().unregisterCurrentThread();

      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      return operationHandle;
    } finally {
     release();
    }
  }

  public OperationHandle getTableTypes() throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      GetTableTypesOperation operation = getOperationManager().newGetTableTypesOperation(this);
      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();
      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      return operationHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getColumns(String catalogName, String schemaName,
      String tableName, String columnName)  throws HiveSQLException {
        OperationHandle operationHandle;
    acquire();
    try {
    GetColumnsOperation operation = getOperationManager().newGetColumnsOperation(this,
        catalogName, schemaName, tableName, columnName);
    //Log Capture
    operationHandle = operation.getHandle();
    getLogManager().registerCurrentThread(operationHandle);
    operation.run();

    // unregister the current thread after capturing the log
    getLogManager().unregisterCurrentThread();
    return operationHandle;
    } finally {
      release();
    }
  }

  public OperationHandle getFunctions(String catalogName, String schemaName, String functionName)
      throws HiveSQLException {
    OperationHandle operationHandle;
    acquire();
    try {
      GetFunctionsOperation operation = getOperationManager()
          .newGetFunctionsOperation(this, catalogName, schemaName, functionName);
      //Log Capture
      operationHandle = operation.getHandle();
      getLogManager().registerCurrentThread(operationHandle);
      operation.run();

      // unregister the current thread after capturing the log
      getLogManager().unregisterCurrentThread();
      return operationHandle;
    } finally {
      release();
    }
  }

  public void close() throws HiveSQLException {
    try {
      acquire();
      /**
       *  For metadata operations like getTables(), getColumns() etc,
       * the session allocates a private metastore handler which should be
       * closed at the end of the session
       */
      if (metastoreClient != null) {
        metastoreClient.close();
      }
      sessionState.close();
      release();
    } catch (IOException ioe) {
      release();
      throw new HiveSQLException("Failure to close", ioe);
    }
  }

  public SessionState getSessionState() {
    return sessionState;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public String setIpAddress(String ipAddress) {
    return this.ipAddress = ipAddress;
  }

  public String getUserName() {
    return username;
  }
  public void setUserName(String userName) {
    this.username = userName;
  }

  @Override
  public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      sessionManager.getOperationManager().cancelOperation(opHandle);
    } finally {
      release();
    }
  }

  @Override
  public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      sessionManager.getOperationManager().closeOperation(opHandle);
    } finally {
      release();
    }
  }

  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      return sessionManager.getOperationManager().getOperationResultSetSchema(opHandle);
    } finally {
      release();
    }
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows)
      throws HiveSQLException {
    acquire();
    try {
      return sessionManager.getOperationManager()
          .getOperationNextRowSet(opHandle, orientation, maxRows);
    } finally {
      release();
    }
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle) throws HiveSQLException {
    acquire();
    try {
      return sessionManager.getOperationManager().getOperationNextRowSet(opHandle);
    } finally {
      release();
    }
  }

  protected HiveSession getSession() {
    return this;
  }
}

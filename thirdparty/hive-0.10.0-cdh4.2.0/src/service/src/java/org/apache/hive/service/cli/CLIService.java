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

package org.apache.hive.service.cli;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;

/**
 * CLIService.
 *
 */
public class CLIService extends CompositeService implements ICLIService {

  private final Log LOG = LogFactory.getLog(CLIService.class.getName());

  private HiveConf hiveConf;
  private SessionManager sessionManager;
  private IMetaStoreClient metastoreClient;


  public CLIService() {
    super("CLIService");
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;

    sessionManager = new SessionManager();
    addService(sessionManager);

    super.init(hiveConf);
  }

  @Override
  public synchronized void start() {
    super.start();

    // Initialize and test a connection to the metastore
    try {
      metastoreClient = new HiveMetaStoreClient(hiveConf);
      metastoreClient.getDatabases("default");
    } catch (Exception e) {
      throw new ServiceException("Unable to connect to MetaStore!", e);
    }
  }

  @Override
  public synchronized void stop() {
    if (metastoreClient != null) {
      metastoreClient.close();
    }
    super.stop();
  }


  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @Override
  public SessionHandle openSession(String username, String password, Map<String, String> configuration)
      throws HiveSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(username, password, configuration);
    LOG.info(sessionHandle + ": openSession()");
    sessionManager.clearThreadLocals();
    return sessionHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeSession(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public void closeSession(SessionHandle sessionHandle)
      throws HiveSQLException {
    sessionManager.closeSession(sessionHandle);
    LOG.info(sessionHandle + ": closeSession()");
    sessionManager.clearThreadLocals();
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getInfo(org.apache.hive.service.cli.SessionHandle, java.util.List)
   */
  @Override
  public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType getInfoType)
      throws HiveSQLException {
    GetInfoValue infoValue = sessionManager.getSession(sessionHandle).getInfo(getInfoType);
    LOG.info(sessionHandle + ": getInfo()");
    sessionManager.clearThreadLocals();
    return infoValue;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#executeStatement(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.util.Map)
   */
  @Override
  public OperationHandle executeStatement(SessionHandle sessionHandle, String statement, Map<String, String> confOverlay)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .executeStatement(statement, confOverlay);
    LOG.info(sessionHandle + ": executeStatement()");
    sessionManager.clearThreadLocals();
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTypeInfo(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getTypeInfo(SessionHandle sessionHandle)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle).getTypeInfo();
    LOG.info(sessionHandle + ": getTypeInfo()");
    sessionManager.clearThreadLocals();
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getCatalogs(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getCatalogs(SessionHandle sessionHandle)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle).getCatalogs();
    LOG.info(sessionHandle + ": getCatalogs()");
    sessionManager.clearThreadLocals();
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getSchemas(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String)
   */
  @Override
  public OperationHandle getSchemas(SessionHandle sessionHandle,
      String catalogName, String schemaName)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getSchemas(catalogName, schemaName);
    LOG.info(sessionHandle + ": getSchemas()");
    sessionManager.clearThreadLocals();
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTables(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.util.List)
   */
  @Override
  public OperationHandle getTables(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, List<String> tableTypes)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager
        .getSession(sessionHandle).getTables(catalogName, schemaName, tableName, tableTypes);
    LOG.info(sessionHandle + ": getTables()");
    sessionManager.clearThreadLocals();
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTableTypes(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getTableTypes(SessionHandle sessionHandle)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle).getTableTypes();
    LOG.info(sessionHandle + ": getTableTypes()");
    sessionManager.clearThreadLocals();
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getColumns(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getColumns(catalogName, schemaName, tableName, columnName);
    LOG.info(sessionHandle + ": getColumns()");
    sessionManager.clearThreadLocals();
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getFunctions(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getFunctions(catalogName, schemaName, functionName);
    LOG.info(sessionHandle + ": getFunctions()");
    sessionManager.clearThreadLocals();
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getOperationStatus(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public OperationState getOperationStatus(OperationHandle opHandle)
      throws HiveSQLException {
    OperationState opState = sessionManager.getOperationManager().getOperationState(opHandle);
    LOG.info(opHandle + ": getOperationStatus()");
    sessionManager.clearThreadLocals();
    return opState;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#cancelOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public void cancelOperation(OperationHandle opHandle)
      throws HiveSQLException {
    sessionManager.getOperationManager().cancelOperation(opHandle);
    LOG.info(opHandle + ": cancelOperation()");
    sessionManager.clearThreadLocals();
    }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public void closeOperation(OperationHandle opHandle)
      throws HiveSQLException {
    sessionManager.getOperationManager().closeOperation(opHandle);
    LOG.info(opHandle + ": closeOperation");
    sessionManager.clearThreadLocals();
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getResultSetMetadata(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws HiveSQLException {
    TableSchema tableSchema = sessionManager.getOperationManager()
        .getOperationResultSetSchema(opHandle);
    LOG.info(opHandle + ": getResultSetMetadata()");
    sessionManager.clearThreadLocals();
    return tableSchema;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#fetchResults(org.apache.hive.service.cli.OperationHandle, org.apache.hive.service.cli.FetchOrientation, long)
   */
  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows)
      throws HiveSQLException {
    RowSet rowSet = sessionManager.getOperationManager()
        .getOperationNextRowSet(opHandle, orientation, maxRows);
    LOG.info(opHandle + ": fetchResults()");
    sessionManager.clearThreadLocals();
    return rowSet;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#fetchResults(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public RowSet fetchResults(OperationHandle opHandle)
      throws HiveSQLException {
    RowSet rowSet = sessionManager.getOperationManager().getOperationNextRowSet(opHandle);
    LOG.info(opHandle + ": fetchResults()");
    sessionManager.clearThreadLocals();
    return rowSet;
  }

  public void setIpAddress(SessionHandle sessionHandle, String ipAddress) {
    try {
      HiveSession session = sessionManager.getSession(sessionHandle);
      session.setIpAddress(ipAddress);
    } catch (HiveSQLException e) {
      // This should not happen
      LOG.error("Unable to get session to set ipAddress", e);
    }
  }

}

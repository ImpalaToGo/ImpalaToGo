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

package org.apache.hive.service.sql;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.service.sql.session.SessionManager;

/**
 * SQLService.
 *
 */
public class SQLService implements ISQLService {

  private final Log LOG = LogFactory.getLog(SQLService.class.getName());

  private final SessionManager sessionManager;


  public SQLService() {
    sessionManager = new SessionManager();
  }


  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @Override
  public SessionHandle openSession(String username, String password, Map<String, String> configuration)
      throws HiveSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(username, password, configuration);
    LOG.info(sessionHandle + ": openSession()");
    return sessionHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#closeSession(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public void closeSession(SessionHandle sessionHandle)
      throws HiveSQLException {
    sessionManager.closeSession(sessionHandle);
    LOG.info(sessionHandle + ": closeSession()");
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getInfo(org.apache.hive.service.sql.SessionHandle, java.util.List)
   */
  @Override
  public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType getInfoType)
      throws HiveSQLException {
    GetInfoValue infoValue = sessionManager.getSession(sessionHandle).getInfo(getInfoType);
    LOG.info(sessionHandle + ": getInfo()");
    return infoValue;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#executeStatement(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.util.Map)
   */
  @Override
  public OperationHandle executeStatement(SessionHandle sessionHandle, String statement, Map<String, String> confOverlay)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .executeStatement(statement, confOverlay);
    LOG.info(sessionHandle + ": executeStatement()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getTypeInfo(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public OperationHandle getTypeInfo(SessionHandle sessionHandle)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle).getTypeInfo();
    LOG.info(sessionHandle + ": getTypeInfo()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getCatalogs(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public OperationHandle getCatalogs(SessionHandle sessionHandle)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle).getCatalogs();
    LOG.info(sessionHandle + ": getCatalogs()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getSchemas(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.lang.String)
   */
  @Override
  public OperationHandle getSchemas(SessionHandle sessionHandle,
      String catalogName, String schemaName)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getSchemas(catalogName, schemaName);
    LOG.info(sessionHandle + ": getSchemas()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getTables(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.util.List)
   */
  @Override
  public OperationHandle getTables(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, List<String> tableTypes)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager
        .getSession(sessionHandle).getTables(catalogName, schemaName, tableName, tableTypes);
    LOG.info(sessionHandle + ": getTables()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getTableTypes(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public OperationHandle getTableTypes(SessionHandle sessionHandle)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle).getTableTypes();
    LOG.info(sessionHandle + ": getTableTypes()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getColumns(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getColumns(catalogName, schemaName, tableName, columnName);
    LOG.info(sessionHandle + ": getColumns()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getFunctions(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName)
      throws HiveSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getFunctions(catalogName, schemaName, functionName);
    LOG.info(sessionHandle + ": getFunctions()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getOperationStatus(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public OperationState getOperationStatus(OperationHandle opHandle)
      throws HiveSQLException {
    OperationState opState = sessionManager.getOperationManager().getOperationState(opHandle);
    LOG.info(opHandle + ": getOperationStatus()");
    return opState;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#cancelOperation(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public void cancelOperation(OperationHandle opHandle)
      throws HiveSQLException {
    sessionManager.getOperationManager().cancelOperation(opHandle);
    LOG.info(opHandle + ": cancelOperation()");
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#closeOperation(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public void closeOperation(OperationHandle opHandle)
      throws HiveSQLException {
    sessionManager.getOperationManager().closeOperation(opHandle);
    LOG.info(opHandle + ": closeOperation");
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getResultSetMetadata(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws HiveSQLException {
    TableSchema tableSchema = sessionManager.getOperationManager()
        .getOperationResultSetSchema(opHandle);
    LOG.info(opHandle + ": getResultSetMetadata()");
    return tableSchema;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#fetchResults(org.apache.hive.service.sql.OperationHandle, org.apache.hive.service.sql.FetchOrientation, long)
   */
  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows)
      throws HiveSQLException {
    RowSet rowSet = sessionManager.getOperationManager()
        .getOperationNextRowSet(opHandle, orientation, maxRows);
    LOG.info(opHandle + ": fetchResults()");
    return rowSet;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#fetchResults(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public RowSet fetchResults(OperationHandle opHandle)
      throws HiveSQLException {
    RowSet rowSet = sessionManager.getOperationManager().getOperationNextRowSet(opHandle);
    LOG.info(opHandle + ": fetchResults()");
    return rowSet;
  }
}

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

/**
 * EmbeddedSQLServiceClient.
 *
 */
public class EmbeddedSQLServiceClient extends SQLServiceClient {
  private final ISQLService sqlService;

  public EmbeddedSQLServiceClient(ISQLService sqlService) {
    this.sqlService = sqlService;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @Override
  public SessionHandle openSession(String username, String password,
      Map<String, String> configuration) throws HiveSQLException {
    return sqlService.openSession(username, password, configuration);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#closeSession(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
    sqlService.closeSession(sessionHandle);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#getInfo(org.apache.hive.service.sql.SessionHandle, java.util.List)
   */
  @Override
  public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType getInfoType)
      throws HiveSQLException {
    return sqlService.getInfo(sessionHandle, getInfoType);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#executeStatement(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.util.Map)
   */
  @Override
  public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws HiveSQLException {
    return sqlService.executeStatement(sessionHandle, statement, confOverlay);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#getTypeInfo(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public OperationHandle getTypeInfo(SessionHandle sessionHandle) throws HiveSQLException {
    return sqlService.getTypeInfo(sessionHandle);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#getCatalogs(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public OperationHandle getCatalogs(SessionHandle sessionHandle) throws HiveSQLException {
    return sqlService.getCatalogs(sessionHandle);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#getSchemas(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.lang.String)
   */
  @Override
  public OperationHandle getSchemas(SessionHandle sessionHandle, String catalogName,
      String schemaName) throws HiveSQLException {
    return sqlService.getSchemas(sessionHandle, catalogName, schemaName);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#getTables(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.util.List)
   */
  @Override
  public OperationHandle getTables(SessionHandle sessionHandle, String catalogName,
      String schemaName, String tableName, List<String> tableTypes) throws HiveSQLException {
    return sqlService.getTables(sessionHandle, catalogName, schemaName, tableName, tableTypes);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#getTableTypes(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public OperationHandle getTableTypes(SessionHandle sessionHandle) throws HiveSQLException {
    return sqlService.getTableTypes(sessionHandle);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#getColumns(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public OperationHandle getColumns(SessionHandle sessionHandle, String catalogName,
      String schemaName, String tableName, String columnName) throws HiveSQLException {
    return sqlService.getColumns(sessionHandle, catalogName, schemaName, tableName, columnName);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#getFunctions(org.apache.hive.service.sql.SessionHandle, java.lang.String)
   */
  @Override
  public OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName)
      throws HiveSQLException {
    return sqlService.getFunctions(sessionHandle, catalogName, schemaName, functionName);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#getOperationStatus(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public OperationState getOperationStatus(OperationHandle opHandle) throws HiveSQLException {
    return sqlService.getOperationStatus(opHandle);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#cancelOperation(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
    sqlService.cancelOperation(opHandle);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#closeOperation(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
    sqlService.closeOperation(opHandle);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#getResultSetMetadata(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle) throws HiveSQLException {
    return sqlService.getResultSetMetadata(opHandle);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#fetchResults(org.apache.hive.service.sql.OperationHandle, org.apache.hive.service.sql.FetchOrientation, long)
   */
  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows)
      throws HiveSQLException {
    return sqlService.fetchResults(opHandle, orientation, maxRows);
  }

}

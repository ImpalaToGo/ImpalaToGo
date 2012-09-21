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
 * SQLServiceClient.
 *
 */
public abstract class SQLServiceClient implements ISQLService {

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @Override
  public abstract SessionHandle openSession(String username, String password,
      Map<String, String> configuration) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#closeSession(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public abstract void closeSession(SessionHandle sessionHandle) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getInfo(org.apache.hive.service.sql.SessionHandle, java.util.List)
   */
  @Override
  public abstract GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType getInfoType)
      throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#executeStatement(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.util.Map)
   */
  @Override
  public abstract OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getTypeInfo(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public abstract OperationHandle getTypeInfo(SessionHandle sessionHandle) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getCatalogs(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public abstract OperationHandle getCatalogs(SessionHandle sessionHandle) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getSchemas(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.lang.String)
   */
  @Override
  public abstract OperationHandle getSchemas(SessionHandle sessionHandle, String catalogName,
      String schemaName) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getTables(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.util.List)
   */
  @Override
  public abstract OperationHandle getTables(SessionHandle sessionHandle, String catalogName,
      String schemaName, String tableName, List<String> tableTypes) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getTableTypes(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public abstract OperationHandle getTableTypes(SessionHandle sessionHandle) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getColumns(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public abstract OperationHandle getColumns(SessionHandle sessionHandle, String catalogName,
      String schemaName, String tableName, String columnName) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getFunctions(org.apache.hive.service.sql.SessionHandle, java.lang.String)
   */
  @Override
  public abstract OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName)
      throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getOperationStatus(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public abstract OperationState getOperationStatus(OperationHandle opHandle) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#cancelOperation(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public abstract void cancelOperation(OperationHandle opHandle) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#closeOperation(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public abstract void closeOperation(OperationHandle opHandle) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getResultSetMetadata(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public abstract TableSchema getResultSetMetadata(OperationHandle opHandle) throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#fetchResults(org.apache.hive.service.sql.OperationHandle, org.apache.hive.service.sql.FetchOrientation, long)
   */
  @Override
  public abstract RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows)
      throws HiveSQLException;

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#fetchResults(org.apache.hive.service.sql.OperationHandle)
   */
  @Override
  public RowSet fetchResults(OperationHandle opHandle) throws HiveSQLException {
    // TODO: provide STATIC default value
    return fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 1000);
  }

}

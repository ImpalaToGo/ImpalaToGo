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

package org.apache.hive.service.sql.thrift;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.sql.FetchOrientation;
import org.apache.hive.service.sql.GetInfoType;
import org.apache.hive.service.sql.GetInfoValue;
import org.apache.hive.service.sql.HiveSQLException;
import org.apache.hive.service.sql.ISQLService;
import org.apache.hive.service.sql.OperationHandle;
import org.apache.hive.service.sql.OperationState;
import org.apache.hive.service.sql.RowSet;
import org.apache.hive.service.sql.SQLService;
import org.apache.hive.service.sql.SessionHandle;
import org.apache.hive.service.sql.TableSchema;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;


/**
 * SQLService.
 *
 */
public class ThriftSQLService implements TSQLService.Iface, Runnable {

  public static final Log LOG = LogFactory.getLog(ThriftSQLService.class.getName());
  /**
   * default port on which to start the Hive server2
   */
  private static final int DEFAULT_SQL_SERVICE_PORT = 10000;

  private final ISQLService service;
  private static final TStatus OK_STATUS = new TStatus(TStatusCode.SUCCESS);
  private static final TStatus ERROR_STATUS = new TStatus(TStatusCode.ERROR);

  private static HiveAuthFactory hiveAuthFactory;

  private int portNum;
  private TServer server;

  private boolean isStarted = false;

  private final HiveConf hiveConf;

  private int minWorkerThreads;
  private int maxWorkerThreads;

  public ThriftSQLService() {
    this(new SQLService());
  }

  public ThriftSQLService(ISQLService service) {
    this.hiveConf = new HiveConf();
    this.service = service;
  }

  private void logRequest(String request) {
    LOG.info(request);
  }

  private void logResponse(String response) {
    LOG.info(response);
  }

  @Override
  public TOpenSessionResp OpenSession(TOpenSessionReq req) throws TException {
    logRequest(req.toString());
    TOpenSessionResp resp = new TOpenSessionResp();
    try {
      String userName;
      if (hiveAuthFactory != null
          && hiveAuthFactory.getRemoteUser() != null) {
        userName = hiveAuthFactory.getRemoteUser();
      } else {
        userName = req.getUsername();
      }
      SessionHandle sessionHandle = service
          .openSession(userName, req.getPassword(), req.getConfiguration());
      resp.setSessionHandle(sessionHandle.toTSessionHandle());
      // TODO: set real configuration map
      resp.setConfiguration(new HashMap<String, String>());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TCloseSessionResp CloseSession(TCloseSessionReq req) throws TException {
    logRequest(req.toString());
    TCloseSessionResp resp = new TCloseSessionResp();
    try {
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      service.closeSession(sessionHandle);
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TGetInfoResp GetInfo(TGetInfoReq req) throws TException {
    logRequest(req.toString());
    TGetInfoResp resp = new TGetInfoResp();
    try {
      GetInfoValue getInfoValue =
          service.getInfo(new SessionHandle(req.getSessionHandle()),
              GetInfoType.getGetInfoType(req.getInfoType()));
      resp.setInfoValue(getInfoValue.toTGetInfoValue());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq req) throws TException {
    logRequest(req.toString());
    TExecuteStatementResp resp = new TExecuteStatementResp();
    try {
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      String statement = req.getStatement();
      Map<String, String> confOverlay = req.getConfOverlay();
      OperationHandle operationHandle =
          service.executeStatement(sessionHandle, statement, confOverlay);
      resp.setOperationHandle(operationHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq req) throws TException {
    logRequest(req.toString());
    TGetTypeInfoResp resp = new TGetTypeInfoResp();
    try {
      OperationHandle operationHandle = service.getTypeInfo(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(operationHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TGetCatalogsResp GetCatalogs(TGetCatalogsReq req) throws TException {
    logRequest(req.toString());
    TGetCatalogsResp resp = new TGetCatalogsResp();
    try {
      OperationHandle opHandle = service.getCatalogs(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TGetSchemasResp GetSchemas(TGetSchemasReq req) throws TException {
    logRequest(req.toString());
    TGetSchemasResp resp = new TGetSchemasResp();
    try {
      OperationHandle opHandle = service.getSchemas(
          new SessionHandle(req.getSessionHandle()), req.getCatalogName(), req.getSchemaName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TGetTablesResp GetTables(TGetTablesReq req) throws TException {
    logRequest(req.toString());
    TGetTablesResp resp = new TGetTablesResp();
    try {
      OperationHandle opHandle = service
          .getTables(new SessionHandle(req.getSessionHandle()), req.getCatalogName(),
              req.getSchemaName(), req.getTableName(), req.getTableTypes());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TGetTableTypesResp GetTableTypes(TGetTableTypesReq req) throws TException {
    logRequest(req.toString());
    TGetTableTypesResp resp = new TGetTableTypesResp();
    try {
      OperationHandle opHandle = service.getTableTypes(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TGetColumnsResp GetColumns(TGetColumnsReq req) throws TException {
    logRequest(req.toString());
    TGetColumnsResp resp = new TGetColumnsResp();
    try {
      OperationHandle opHandle = service.getColumns(
          new SessionHandle(req.getSessionHandle()),
          req.getCatalogName(),
          req.getSchemaName(),
          req.getTableName(),
          req.getColumnName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TGetFunctionsResp GetFunctions(TGetFunctionsReq req) throws TException {
    logRequest(req.toString());
    TGetFunctionsResp resp = new TGetFunctionsResp();
    try {
      OperationHandle opHandle = service.getFunctions(
          new SessionHandle(req.getSessionHandle()), req.getCatalogName(),
          req.getSchemaName(), req.getFunctionName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq req) throws TException {
    logRequest(req.toString());
    TGetOperationStatusResp resp = new TGetOperationStatusResp();
    try {
      OperationState operationState = service.getOperationStatus(new OperationHandle(req.getOperationHandle()));
      resp.setOperationState(operationState.toTOperationState());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TCancelOperationResp CancelOperation(TCancelOperationReq req) throws TException {
    logRequest(req.toString());
    TCancelOperationResp resp = new TCancelOperationResp();
    try {
      service.cancelOperation(new OperationHandle(req.getOperationHandle()));
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TCloseOperationResp CloseOperation(TCloseOperationReq req) throws TException {
    logRequest(req.toString());
    TCloseOperationResp resp = new TCloseOperationResp();
    try {
      service.closeOperation(new OperationHandle(req.getOperationHandle()));
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq req)
      throws TException {
    logRequest(req.toString());
    TGetResultSetMetadataResp resp = new TGetResultSetMetadataResp();
    try {
      TableSchema schema = service.getResultSetMetadata(new OperationHandle(req.getOperationHandle()));
      resp.setSchema(schema.toTTableSchema());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }

  @Override
  public TFetchResultsResp FetchResults(TFetchResultsReq req) throws TException {
    logRequest(req.toString());
    TFetchResultsResp resp = new TFetchResultsResp();
    try {
      RowSet rowSet = service.fetchResults(
          new OperationHandle(req.getOperationHandle()),
          FetchOrientation.getFetchOrientation(req.getOrientation()),
          req.getMaxRows());
      resp.setResults(rowSet.toTRowSet());
      resp.setHasMoreRows(false);
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      e.printStackTrace();
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    logResponse(resp.toString());
    return resp;
  }


  /**
   * @param args
   * @throws TTransportException
   */
  public static void main(String[] args) {
    new ThriftSQLService().start();
  }

  public void setPortNumber(int portNum) {
    this.portNum = portNum;
  }

  public synchronized void start() {
    if (!isStarted) {
      new Thread(this).start();
      isStarted = true;
    }
  }

  public synchronized void stop() {
    if (isStarted) {
      server.stop();
      isStarted = false;
    }
  }

  @Override
  public void run() {
    try {
      hiveAuthFactory = new HiveAuthFactory();
      TTransportFactory  transportFactory = hiveAuthFactory.getAuthTransFactory();
      TProcessorFactory processorFactory = hiveAuthFactory.getAuthProcFactory(service);

      String hivePort = System.getenv("HIVE_SERVER2_PORT");
      if (hivePort == null) {
        portNum = DEFAULT_SQL_SERVICE_PORT;
      } else {
        portNum = Integer.valueOf(hivePort);
      }

      String hiveHost = System.getenv("HIVE_SERVER2_BIND_HOST");
      InetSocketAddress serverAddress;
      if (hiveHost == null) {
        serverAddress = new  InetSocketAddress(portNum);
        hiveHost = "localhost";
      } else {
        serverAddress = new InetSocketAddress(hiveHost, portNum);
      }

      minWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS);
      maxWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS);

      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(new TServerSocket(serverAddress))
      .processorFactory(processorFactory)
      .transportFactory(transportFactory)
      .protocolFactory(new TBinaryProtocol.Factory())
      .minWorkerThreads(minWorkerThreads)
      .maxWorkerThreads(maxWorkerThreads);

      server = new TThreadPoolServer(sargs);

      System.err.println("ThriftSQLService listening on " + hiveHost + ":" + portNum);

      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}

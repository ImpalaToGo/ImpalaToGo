package org.apache.hive.service.cli.operation;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

public class AsyncExecStmtOperation extends ExecuteStatementOperation {
  private ExecuteStatementOperation execOP;
  private final ExecutorService opExecutor;
  private Future<String> execFuture = null;
  private static final Log LOG = LogFactory.getLog(AsyncExecStmtOperation.class);

  public AsyncExecStmtOperation(HiveSession parentSession, String statement,
      Map<String, String> confOverlay) {
    super(parentSession, statement, confOverlay);
    opExecutor = Executors.newSingleThreadExecutor();
    LOG.info("Got ansync exec for query " + statement);
  }

  public static AsyncExecStmtOperation wrapExecStmtOperation(ExecuteStatementOperation execOP) {
    AsyncExecStmtOperation newExecOP =
          new AsyncExecStmtOperation(execOP.getParentSession(), execOP.getStatement(), execOP.confOverlay);
    newExecOP.setExecOP(execOP);
    return newExecOP;
  }

  private void startLogCapture(HiveSession parentSession, OperationHandle opHandle) throws HiveSQLException {
    parentSession.getSessionManager().getLogManager().registerCurrentThread(opHandle);
  }

  private void stopLogCapture(HiveSession parentSession) {
    parentSession.getSessionManager().getLogManager().unregisterCurrentThread();
  }

  @Override
  public void run() throws HiveSQLException {
    prepare();
    final ExecuteStatementOperation currExec = execOP;
    final HiveSession parentSession = getParentSession();
    final OperationHandle parentHandle = getHandle();
    execFuture = opExecutor.submit(new Callable<String>() {
      public String  call() throws HiveSQLException {
        startLogCapture(parentSession, parentHandle);
        currExec.run();
        stopLogCapture(parentSession);
        return null;
      }
    });
  }

  @Override
  public void prepare() throws HiveSQLException {
    final ExecuteStatementOperation currExec = execOP;
    final HiveSession parentSession = getParentSession();
    final OperationHandle parentHandle = getHandle();
    execFuture = opExecutor.submit(new Callable<String>() {
      public String  call() throws HiveSQLException {
        // Clone the current configuration for an async query. we don't want the
        // query to see the config changes after the query starts to execute
        startLogCapture(parentSession, parentHandle);
        HiveConf queryConf = new HiveConf(getParentSession().getHiveConf());
        SessionState.start(currExec.getParentSession().getSessionState());
        currExec.prepare(queryConf);
        stopLogCapture(parentSession);
        return null;
      }
    });
    waitForCompletion(execFuture);
    setHasResultSet(execOP.hasResultSet());
  }

  @Override
  public void close() throws HiveSQLException {
    // wait for the async statement to finish
    waitForCompletion(execFuture);
    final ExecuteStatementOperation currExec = execOP;
    final HiveSession parentSession = getParentSession();
    final OperationHandle parentHandle = getHandle();
    Future<String> opFuture = opExecutor.submit(new Callable<String>() {
      public String  call() throws HiveSQLException {
        startLogCapture(parentSession, parentHandle);
        currExec.close();
        stopLogCapture(parentSession);
        return null;
      }
    });
    waitForCompletion(opFuture);
    opExecutor.shutdown();
  }

  @Override
  public void cancel() throws HiveSQLException {
    final ExecuteStatementOperation currExec = execOP;
    final HiveSession parentSession = getParentSession();
    final OperationHandle parentHandle = getHandle();
    Future<String> opFuture = opExecutor.submit(new Callable<String>() {
      public String  call() throws HiveSQLException {
        startLogCapture(parentSession, parentHandle);
        currExec.cancel();
        stopLogCapture(parentSession);
        return null;
      }
    });
    waitForCompletion(opFuture);
    opExecutor.shutdown();
  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    return execOP.getResultSetSchema();
  }

  @Override
  public RowSet getNextRowSet(final FetchOrientation orientation, final long maxRows)
          throws HiveSQLException {
    checkExecutionStatus();
    final ExecuteStatementOperation currExec = execOP;
    final HiveSession parentSession = getParentSession();
    final OperationHandle parentHandle = getHandle();
    Future<RowSet> opFuture = opExecutor.submit(new Callable<RowSet>() {
      public RowSet call() throws HiveSQLException {
        startLogCapture(parentSession, parentHandle);
        RowSet rowSet = currExec.getNextRowSet(orientation, maxRows);
        stopLogCapture(parentSession);
        return rowSet;
      }
    });
    return waitForCompletion(opFuture);
  }

  @Override
  public OperationState getState() {
    return execOP.getState();
  }

  @Override
  public boolean isPrepared() {
    return execOP.isPrepared();
  }

  // check if the query is still running or failed
  private void checkExecutionStatus() throws HiveSQLException {
    if (execFuture == null) {
      // no background thread running. 'Invalid cursor state' exception
      throw new HiveSQLException("No background query executed", "24000");
    }
    if (getState().equals(OperationState.RUNNING) || !execFuture.isDone()) {
      throw new HiveSQLException("Query still runing", "HY010");
    }
    waitForCompletion(execFuture);
    if (getState().equals(OperationState.ERROR)) {
      throw new HiveSQLException("Query execution failed", "07000");
    }
    if (getState().equals(OperationState.CANCELED)) {
      // query is already canceled. 'Invalid cursor state' exception
      throw new HiveSQLException("Query execution was canceled", "24000");
    }
  }

  // wait for the given future to complete
  private <T> T waitForCompletion(Future<T> opFuture)
        throws HiveSQLException {
    T result = null;
    try {
      result = opFuture.get();
    } catch (InterruptedException e) {
      throw  new HiveSQLException(e.getMessage(), "24000", -1, e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof HiveSQLException) {
        throw (HiveSQLException)e.getCause();
      } else {
        throw  new HiveSQLException(e.getMessage(), e.getCause());
      }
    }
    return result;
  }

  private void setExecOP(ExecuteStatementOperation execOP) {
    this.execOP = execOP;
  }
}

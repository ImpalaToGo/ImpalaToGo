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

package org.apache.hive.service.cli.log;


import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * LogManager - LogManager is responsible for managing the lifecycle of in memory operation logs for HS2.
 * Each log object is maintained as a rolling log whose size can't exceed 1MB.
 * LogManager tracks the log objects by operation handle as well as by the thread whose output will
 * be redirected to these log objects.
 */
public class LogManager extends AbstractService {
  private HiveConf hiveConf;

  private final Map<OperationHandle, OperationLog> OperationLogByOperation =
      new ConcurrentHashMap<OperationHandle, OperationLog> ();
  private final Map<String, OperationLog> OperationLogByThreadName =
      new ConcurrentHashMap<String, OperationLog> ();

  private boolean isOperationLogCaptureIntialized = false;

  private static final String DEFAULT_LAYOUT_PATTERN = "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n";

  private static Logger LOG = Logger.getLogger(LogManager.class.getName());
  private SessionManager sessionManager;

  public LogManager() {
    super("LogManager");
  }

  public void setSessionManager(SessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  public SessionManager getSessionManager() {
    return sessionManager;
  }

  public void initOperationLogCapture() {
    if (isOperationLogCaptureIntialized) {
      return;
    }

    // There should be a ConsoleAppender. Copy its Layout.
    Logger root = Logger.getRootLogger();
    Layout layout = null;

    Enumeration<?> appenders = root.getAllAppenders();
    while (appenders.hasMoreElements()) {
      Appender ap = (Appender) appenders.nextElement();
      if (ap.getClass().equals(ConsoleAppender.class)) {
        layout = ap.getLayout();
        break;
      }
    }

    if (layout == null) {
      layout = new PatternLayout(DEFAULT_LAYOUT_PATTERN);
      LOG.info("Cannot find a Layout from a ConsoleAppender. Using default Layout pattern.");
    }

    // Register another Appender (with the same layout) that talks to us.
    Appender ap = new LogDivertAppender(layout, this);
    root.addAppender(ap);

    isOperationLogCaptureIntialized = true;
  }

  public OperationLog createNewOperationLog(OperationHandle operationHandle, String name) {
    int size = HiveConf.getIntVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_IN_MEM_LOG_SIZE);
    LOG.info("Operation log size: " + size);
    OperationLog OperationLog = new OperationLog(name, size);
    OperationLogByOperation.put(operationHandle, OperationLog);
    return OperationLog;
  }

  public boolean destroyOperationLog(OperationHandle operationHandle) {
    OperationLog OperationLog = OperationLogByOperation.remove(operationHandle);
    if (OperationLog == null) {
      LOG.debug("No OperationLog found for operation: " + operationHandle.hashCode());
      return false;
    }
    return true;
  }

  public void registerCurrentThread(OperationHandle operationHandle) throws HiveSQLException {
    String threadName = Thread.currentThread().getName();

    OperationLog OperationLog = getOperationLogByOperation(operationHandle, true);

    if (OperationLogByThreadName.containsKey(threadName)) {
      LOG.debug("Thread: " + threadName + " is already registered.");
    }
    OperationLogByThreadName.put(threadName, OperationLog);
  }

  public void registerCurrentThread(OperationLog OperationLog) {
    String threadName = Thread.currentThread().getName();
    OperationLogByThreadName.put(threadName, OperationLog);
  }

  public boolean unregisterCurrentThread() {
    String threadName = Thread.currentThread().getName();
    OperationLog OperationLog = OperationLogByThreadName.remove(threadName);
    if (OperationLog == null) {
      LOG.debug("Failed to unregister thread " + threadName + ": OperationLog object is currently "
          + "not regsitered");
      return false;
    }
    return true;
  }

  public OperationLog getOperationLogByThreadName(String threadName) {
    OperationLog OperationLog = OperationLogByThreadName.get(threadName);
    if (OperationLog == null) {
      LOG.debug("Operation log assocaited with thread: " + threadName + " couldn't be found.");
    }
    return OperationLog;
  }

  public OperationLog getOperationLogByOperation(OperationHandle operationHandle,
    boolean createIfAbsent) throws HiveSQLException {
    OperationLog operationLog = OperationLogByOperation.get(operationHandle);
    if (operationLog == null && createIfAbsent) {
      operationLog = createNewOperationLog(operationHandle, operationHandle.toString());
    } else if (operationLog == null) {
      throw new HiveSQLException("Couldn't find log associated with operation handle: " +
        operationHandle.toString());
    }
    return operationLog;
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    super.init(hiveConf);
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_IN_MEM_LOGGING)) {
      initOperationLogCapture();
    } else {
      LOG.info("Opeation level logging is turned off");
    }
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }
}

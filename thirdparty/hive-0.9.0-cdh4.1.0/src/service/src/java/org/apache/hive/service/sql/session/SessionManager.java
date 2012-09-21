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
import java.util.Map;

import org.apache.hive.service.sql.HiveSQLException;
import org.apache.hive.service.sql.SessionHandle;
import org.apache.hive.service.sql.operation.OperationManager;

/**
 * SessionManager.
 *
 */
public class SessionManager {

  Map<SessionHandle, Session> handleToSession = new HashMap<SessionHandle, Session>();
  OperationManager operationManager = new OperationManager();

  public SessionHandle openSession(String username, String password, Map<String, String> sessionConf) {
    Session session = new Session(username, password, sessionConf);
    session.setSessionManager(this);
    session.setOperationManager(operationManager);
    handleToSession.put(session.getSessionHandle(), session);
    return session.getSessionHandle();
  }

  public void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
    Session session = handleToSession.remove(sessionHandle);
    if (session == null) {
      throw new HiveSQLException("Session does not exist!");
    }
    session.close();
  }


  public Session getSession(SessionHandle sessionHandle) throws HiveSQLException {
    Session session = handleToSession.get(sessionHandle);
    if (session == null) {
      throw new HiveSQLException("Invalid SessionHandle: " + sessionHandle);
    }
    return session;
  }

  public OperationManager getOperationManager() {
    return operationManager;
  }

}

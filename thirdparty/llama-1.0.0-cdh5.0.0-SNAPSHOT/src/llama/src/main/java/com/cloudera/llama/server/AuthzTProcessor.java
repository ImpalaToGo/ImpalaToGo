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
package com.cloudera.llama.server;

import org.apache.hadoop.security.Groups;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class AuthzTProcessor implements TProcessor {
  private static final Logger LOG =
      LoggerFactory.getLogger(AuthzTProcessor.class);

  private final Groups groupsMapping;
  private final TProcessor tProcessor;
  private final String userType;
  private final Set<String> usersACL;
  private final Set<String> groupsACL;
  private final boolean allAllowed;

  public AuthzTProcessor(ServerConfiguration sConf, boolean isAdmin,
      TProcessor tProcessor) {
    userType = (isAdmin) ? "admin" : "client";
    groupsMapping = new Groups(sConf.getConf());
    String[] users = (isAdmin) ? sConf.getAdminUserACL() :
                     sConf.getClientUserACL();
    String[] groups = (isAdmin) ? sConf.getAdminGroupACL() :
                      sConf.getClientGroupACL();
    allAllowed = users == null && groups == null;
    usersACL = new TreeSet<String>();
    groupsACL = new TreeSet<String>();
    if (!allAllowed) {
      if (users != null) {
        for (String u : users) {
          usersACL.add(u);
        }
      }
      if (groups != null) {
        for (String g : groups) {
          groupsACL.add(g);
        }
      }
    } else {
      LOG.warn(
          "Authorization enforcement is disabled, '{}' user ACL set to '{}'",
          userType, ServerConfiguration.ACL_DEFAULT);
    }
    this.tProcessor = tProcessor;
  }

  @Override
  public boolean process(TProtocol inProt, TProtocol outProt)
      throws TException {
    TSaslServerTransport saslServerTransport = (TSaslServerTransport)
        inProt.getTransport();
    String principal = saslServerTransport.getSaslServer().getAuthorizationID();
    try {
      if (isAuthorized(principal)) {
        LOG.debug("Authorization for '{}' as '{}' user, OK", principal,
            userType);
      } else {
        LOG.warn("Authorization for '{}' as '{}' user, FAILED", principal,
            userType);
        throw new TException("Unauthorized");
      }
      return tProcessor.process(inProt, outProt);
    } catch (IOException ex) {
      LOG.error("Could not verify authorization, {}", ex.toString(), ex);
      throw new TException(ex);
    }
  }

  protected boolean isAuthorized(String principal) throws IOException {
    boolean authorized = true;
    if (!allAllowed) {
      int i = principal.indexOf("/");
      if (i > -1) {
        principal = principal.substring(0, i);
      } else {
        i = principal.indexOf("@");
        if (i > -1) {
          principal = principal.substring(0, i);
        }
      }
      authorized = usersACL.contains(principal);
      if (!authorized) {
        authorized = anyUserGroupInACL(principal);
      }
    }
    return authorized;
  }

  protected boolean anyUserGroupInACL(String principal) throws IOException {
    List<String> groups = groupsMapping.getGroups(principal);
    boolean groupInACL = false;
    for (String g : groups) {
      groupInACL = groupsACL.contains(g);
      if (groupInACL) {
        break;
      }
    }
    return groupInACL;
  }
}

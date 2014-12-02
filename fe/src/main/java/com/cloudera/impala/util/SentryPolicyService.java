// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.SentryConfig;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.RolePrivilege;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TPrivilege;
import com.cloudera.impala.thrift.TPrivilegeLevel;
import com.cloudera.impala.thrift.TPrivilegeScope;
import com.google.common.collect.Lists;

/**
 *  Wrapper around the SentryService APIs that are used by Impala and Impala tests.
 */
public class SentryPolicyService {
  private final static Logger LOG = LoggerFactory.getLogger(SentryPolicyService.class);
  private final String ACCESS_DENIED_ERROR_MSG =
      "User '%s' does not have privileges to execute: %s";
  private final SentryConfig config_;

  /**
   * Wrapper around a SentryPolicyServiceClient.
   * TODO: When SENTRY-296 is resolved we can more easily cache connections instead of
   * opening a new connection for each request.
   */
  class SentryServiceClient {
    private final SentryPolicyServiceClient client_;

    /**
     * Creates and opens a new Sentry Service thrift client.
     */
    public SentryServiceClient() throws InternalException {
      client_ = createClient();
    }

    /**
     * Get the underlying SentryPolicyServiceClient.
     */
    public SentryPolicyServiceClient get() {
      return client_;
    }

    /**
     * Returns this client back to the connection pool. Can be called multiple times.
     */
    public void close() {
      client_.close();
    }

    /**
     * Creates a new client to the SentryService.
     */
    private SentryPolicyServiceClient createClient() throws InternalException {
      SentryPolicyServiceClient client;
      try {
        client = new SentryPolicyServiceClient(config_.getConfig());
      } catch (IOException e) {
        throw new InternalException("Error creating Sentry Service client: ", e);
      }
      return client;
    }
  }

  public SentryPolicyService(SentryConfig config) {
    config_ = config;
  }

  /**
   * Drops a role.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to drop.
   * @param ifExists - If true, no error is thrown if the role does not exist.
   * @throws ImpalaException - On any error dropping the role.
   */
  public void dropRole(User requestingUser, String roleName, boolean ifExists)
      throws ImpalaException {
    LOG.trace(String.format("Dropping role: %s on behalf of: %s", roleName,
        requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      if (ifExists) {
        client.get().dropRoleIfExists(requestingUser.getShortName(), roleName);
      } else {
        client.get().dropRole(requestingUser.getShortName(), roleName);
      }
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "DROP_ROLE"));
    } catch (SentryUserException e) {
      throw new InternalException("Error dropping role: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Creates a new role.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to create.
   * @param ifNotExists - If true, no error is thrown if the role already exists.
   * @throws ImpalaException - On any error creating the role.
   */
  public void createRole(User requestingUser, String roleName, boolean ifNotExists)
      throws ImpalaException {
    LOG.trace(String.format("Creating role: %s on behalf of: %s", roleName,
        requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().createRole(requestingUser.getShortName(), roleName);
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "CREATE_ROLE"));
    } catch (SentryAlreadyExistsException e) {
      if (ifNotExists) return;
      throw new InternalException("Error creating role: ", e);
    } catch (SentryUserException e) {
      throw new InternalException("Error creating role: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Grants a role to a group.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to grant to a group. Role must already exist.
   * @param groupName - The group to grant the role to.
   * @throws ImpalaException - On any error.
   */
  public void grantRoleToGroup(User requestingUser, String roleName, String groupName)
      throws ImpalaException {
    LOG.trace(String.format("Granting role '%s' to group '%s' on behalf of: %s",
        roleName, groupName, requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().grantRoleToGroup(requestingUser.getShortName(), groupName, roleName);
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "GRANT_ROLE"));
    } catch (SentryUserException e) {
      throw new InternalException(
          "Error making 'grantRoleToGroup' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }


  /**
   * Removes a role from a group.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role name to remove.
   * @param groupName - The group to remove the role from.
   * @throws InternalException - On any error.
   */
  public void revokeRoleFromGroup(User requestingUser, String roleName, String groupName)
      throws ImpalaException {
    LOG.trace(String.format("Revoking role '%s' from group '%s' on behalf of: %s",
        roleName, groupName, requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      client.get().revokeRoleFromGroup(requestingUser.getShortName(),
          groupName, roleName);
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "REVOKE_ROLE"));
    } catch (SentryUserException e) {
      throw new InternalException(
          "Error making 'revokeRoleFromGroup' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Grants privileges to an existing role.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to grant privileges to (case insensitive).
   * @param privilege - The privilege to grant.
   * @throws ImpalaException - On any error
   */
  public void grantRolePrivilege(User requestingUser, String roleName,
      TPrivilege privilege) throws ImpalaException {
    LOG.trace(String.format("Granting role '%s' privilege '%s' on '%s' on behalf of: %s",
        roleName, privilege.toString(), privilege.getScope().toString(),
        requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      switch (privilege.getScope()) {
        case SERVER:
          client.get().grantServerPrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.isHas_grant_opt());
          break;
        case DATABASE:
          client.get().grantDatabasePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getDb_name(),
              privilege.getPrivilege_level().toString(),
              privilege.isHas_grant_opt());
          break;
        case TABLE:
          String tblName = privilege.getTable_name();
          String dbName = privilege.getDb_name();
          client.get().grantTablePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), dbName, tblName,
              privilege.getPrivilege_level().toString(),
              privilege.isHas_grant_opt());
          break;
        case URI:
          client.get().grantURIPrivilege(requestingUser.getShortName(),
              roleName, privilege.getServer_name(), privilege.getUri(),
              privilege.isHas_grant_opt());
          break;
      }
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "GRANT_PRIVILEGE"));
    } catch (SentryUserException e) {
      throw new InternalException(
          "Error making 'grantPrivilege*' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Revokes privileges from an existing role.
   *
   * @param requestingUser - The requesting user.
   * @param roleName - The role to grant privileges to (case insensitive).
   * @param privilege - The privilege to grant to the object.
   * @throws ImpalaException - On any error
   */
  public void revokeRolePrivilege(User requestingUser, String roleName,
      TPrivilege privilege) throws ImpalaException {
    LOG.trace(String.format("Revoking role '%s' privilege '%s' on '%s' on behalf of: %s",
        roleName, privilege.toString(), privilege.getScope().toString(),
        requestingUser.getName()));
    SentryServiceClient client = new SentryServiceClient();
    try {
      switch (privilege.getScope()) {
        case SERVER:
          client.get().revokeServerPrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), null);
          break;
        case DATABASE:
          client.get().revokeDatabasePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), privilege.getDb_name(),
              privilege.getPrivilege_level().toString(), null);
          break;
        case TABLE:
          String tblName = privilege.getTable_name();
          String dbName = privilege.getDb_name();
          client.get().revokeTablePrivilege(requestingUser.getShortName(), roleName,
              privilege.getServer_name(), dbName, tblName,
              privilege.getPrivilege_level().toString(),
              null);
          break;
        case URI:
          client.get().revokeURIPrivilege(requestingUser.getShortName(),
              roleName, privilege.getServer_name(), privilege.getUri(),
              null);
          break;
      }
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "REVOKE_PRIVILEGE"));
    } catch (SentryUserException e) {
      throw new InternalException(
          "Error making 'revokePrivilege*' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Lists all roles granted to all groups a user belongs to.
   */
  public List<TSentryRole> listUserRoles(User requestingUser)
      throws ImpalaException {
    SentryServiceClient client = new SentryServiceClient();
    try {
      return Lists.newArrayList(client.get().listUserRoles(
          requestingUser.getShortName()));
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "LIST_USER_ROLES"));
    } catch (SentryUserException e) {
      throw new InternalException(
          "Error making 'listUserRoles' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Lists all roles.
   */
  public List<TSentryRole> listAllRoles(User requestingUser) throws ImpalaException {
    SentryServiceClient client = new SentryServiceClient();
    try {
      return Lists.newArrayList(client.get().listRoles(requestingUser.getShortName()));
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "LIST_ROLES"));
    } catch (SentryUserException e) {
      throw new InternalException("Error making 'listRoles' RPC to Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Lists all privileges granted to a role.
   */
  public List<TSentryPrivilege> listRolePrivileges(User requestingUser, String roleName)
      throws ImpalaException {
    SentryServiceClient client = new SentryServiceClient();
    try {
      return Lists.newArrayList(client.get().listAllPrivilegesByRoleName(
          requestingUser.getShortName(), roleName));
    } catch (SentryAccessDeniedException e) {
      throw new AuthorizationException(String.format(ACCESS_DENIED_ERROR_MSG,
          requestingUser.getName(), "LIST_ROLE_PRIVILEGES"));
    } catch (SentryUserException e) {
      throw new InternalException("Error making 'listAllPrivilegesByRoleName' RPC to " +
          "Sentry Service: ", e);
    } finally {
      client.close();
    }
  }

  /**
   * Utility function that converts a TSentryPrivilege to an Impala TPrivilege object.
   */
  public static TPrivilege sentryPrivilegeToTPrivilege(TSentryPrivilege sentryPriv) {
    TPrivilege privilege = new TPrivilege();
    privilege.setServer_name(sentryPriv.getServerName());
    if (sentryPriv.isSetDbName()) privilege.setDb_name(sentryPriv.getDbName());
    if (sentryPriv.isSetTableName()) privilege.setTable_name(sentryPriv.getTableName());
    if (sentryPriv.isSetURI()) privilege.setUri(sentryPriv.getURI());
    privilege.setScope(Enum.valueOf(TPrivilegeScope.class,
        sentryPriv.getPrivilegeScope().toUpperCase()));
    if (sentryPriv.getAction().equals("*")) {
      privilege.setPrivilege_level(TPrivilegeLevel.ALL);
    } else {
      privilege.setPrivilege_level(Enum.valueOf(TPrivilegeLevel.class,
          sentryPriv.getAction().toUpperCase()));
    }
    privilege.setPrivilege_name(RolePrivilege.buildRolePrivilegeName(privilege));
    privilege.setCreate_time_ms(sentryPriv.getCreateTime());
    if (sentryPriv.isSetGrantOption() &&
        sentryPriv.getGrantOption() == TSentryGrantOption.TRUE) {
      privilege.setHas_grant_opt(true);
    } else {
      privilege.setHas_grant_opt(false);
    }
    return privilege;
  }

  // Dummy Sentry class to allow compilation on CDH4.
  public static class SentryUserException extends Exception {
    private static final long serialVersionUID = 9012029067485961905L;
  }

  // Dummy Sentry class to allow compilation on CDH4.
  public static class SentryAlreadyExistsException extends Exception {
    private static final long serialVersionUID = 9012029067485961905L;
  }

  //Dummy Sentry class to allow compilation on CDH4.
  public static class SentryAccessDeniedException extends Exception {
    private static final long serialVersionUID = 9012029067485961905L;
  }

  // Dummy Sentry class to allow compilation on CDH4.
  public abstract static class TSentryRole {
    public abstract List<TSentryGroup> getGroups();
    public abstract String getRoleName();
  }

  //Dummy Sentry class to allow compilation on CDH4.
  public abstract static class TSentryGroup {
    public abstract String getGroupName();
  }

  // Dummy Sentry enum to allow compilation on CDH4.
  enum TSentryGrantOption {
    TRUE,
    FALSE,
    UNSET
  }

  // Dummy Sentry class to allow compilation on CDH4.
  public abstract static class TSentryPrivilege {
    public abstract boolean isSetDbName();
    public abstract boolean isSetTableName();
    public abstract boolean isSetServerName();
    public abstract boolean isSetURI();

    public abstract String getAction();
    public abstract String getPrivilegeScope();
    public abstract String getTableName();
    public abstract String getDbName();
    public abstract String getServerName();
    public abstract String getURI();
    public abstract long getCreateTime();
    public abstract boolean isSetGrantOption();
    public abstract TSentryGrantOption getGrantOption();
  }

  // Dummy Sentry class to allow compilation on CDH4.
  public static class SentryPolicyServiceClient {
    public SentryPolicyServiceClient(Configuration config) throws IOException {
    }

    public void close() {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void createRole(String user, String roleName) throws SentryUserException,
        SentryAlreadyExistsException, SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void dropRole(String user, String roleName) throws SentryUserException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void dropRoleIfExists(String user, String roleName)
        throws SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public List<TSentryRole> listRoles(String user)
        throws SentryUserException, SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public List<TSentryRole> listUserRoles(String user)
        throws SentryUserException, SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public List<TSentryPrivilege> listAllPrivilegesByRoleName(String user, String role)
        throws SentryUserException, SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void grantTablePrivilege(String user, String roleName,
        String serverName, String dbName, String tableName, String privilege,
        Boolean grantOpt)
        throws SentryUserException, SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void revokeTablePrivilege(String user, String roleName,
        String serverName, String dbName, String tableName, String privilege,
        Boolean grantOpt) throws SentryUserException, SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void grantDatabasePrivilege(String user, String roleName,
        String serverName, String dbName, String privilege, Boolean grantOpt)
        throws SentryUserException, SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void revokeDatabasePrivilege(String user, String roleName,
        String serverName, String dbName, String privilege, Boolean grantOpt)
        throws SentryUserException, SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void grantServerPrivilege(String user, String roleName,
        String serverName, Boolean grantOpt) throws SentryUserException,
        SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void revokeServerPrivilege(String user, String roleName,
        String serverName, Boolean grantOpt) throws SentryUserException,
        SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void grantURIPrivilege(String user, String roleName,
        String serverName, String dbName, Boolean grantOpt) throws SentryUserException,
        SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void revokeURIPrivilege(String user, String roleName,
        String serverName, String dbName, Boolean grantOpt) throws SentryUserException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void grantRoleToGroup(String user, String roleName, String group)
        throws SentryUserException, SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }

    public void revokeRoleFromGroup(String user, String role, String group)
        throws SentryUserException, SentryAccessDeniedException {
      throw new UnsupportedOperationException("Sentry Service is not supported on CDH4");
    }
  }
}

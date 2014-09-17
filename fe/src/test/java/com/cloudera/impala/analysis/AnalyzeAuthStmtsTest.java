// Copyright (c) 2014 Cloudera, Inc. All rights reserved.
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

package com.cloudera.impala.analysis;

import java.util.HashSet;

import org.junit.Test;

import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Role;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.testutil.TestUtils;
import com.cloudera.impala.thrift.TQueryCtx;

public class AnalyzeAuthStmtsTest extends AnalyzerTest {
  public AnalyzeAuthStmtsTest() throws AnalysisException {
    analyzer_ = createAnalyzer(Catalog.DEFAULT_DB);
    analyzer_.getCatalog().getAuthPolicy().addRole(
        new Role("myRole", new HashSet<String>()));
  }

  @Override
  protected Analyzer createAnalyzer(String defaultDb) {
    TQueryCtx queryCtx =
        TestUtils.createQueryContext(defaultDb, System.getProperty("user.name"));
    return new Analyzer(catalog_, queryCtx,
        AuthorizationConfig.createHadoopGroupAuthConfig("server1", null, null));
  }

  private Analyzer createAuthDisabledAnalyzer(String defaultDb) {
    TQueryCtx queryCtx =
        TestUtils.createQueryContext(defaultDb, System.getProperty("user.name"));
    return new Analyzer(catalog_, queryCtx,
        AuthorizationConfig.createAuthDisabledConfig());
  }

  @Test
  public void AnalyzeShowRoles() {
    AnalysisError("SHOW ROLES", "Sentry Service is not supported on CDH4");
    AnalysisError("SHOW ROLE GRANT GROUP myGroup",
        "Sentry Service is not supported on CDH4");
    AnalysisError("SHOW CURRENT ROLES", "Sentry Service is not supported on CDH4");

    Analyzer authDisabledAnalyzer = createAuthDisabledAnalyzer(Catalog.DEFAULT_DB);
    AnalysisError("SHOW ROLES", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("SHOW ROLE GRANT GROUP myGroup", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("SHOW CURRENT ROLES", authDisabledAnalyzer,
        "Authorization is not enabled.");
  }

  @Test
  public void AnalyzeShowGrantRole() {
    AnalysisError("SHOW GRANT ROLE myRole", "Sentry Service is not supported on CDH4");
    AnalysisError("SHOW GRANT ROLE myRole ON SERVER",
        "Sentry Service is not supported on CDH4");
    AnalysisError("SHOW GRANT ROLE myRole ON DATABASE functional",
        "Sentry Service is not supported on CDH4");
    AnalysisError("SHOW GRANT ROLE myRole ON TABLE foo",
        "Sentry Service is not supported on CDH4");
    AnalysisError("SHOW GRANT ROLE myRole ON TABLE functional.alltypes",
        "Sentry Service is not supported on CDH4");
    AnalysisError("SHOW GRANT ROLE myRole ON URI 'hdfs:////test-warehouse//foo'",
        "Sentry Service is not supported on CDH4");
    AnalysisError("SHOW GRANT ROLE does_not_exist",
        "Sentry Service is not supported on CDH4");
    AnalysisError("SHOW GRANT ROLE does_not_exist ON SERVER",
        "Sentry Service is not supported on CDH4");

    Analyzer authDisabledAnalyzer = createAuthDisabledAnalyzer(Catalog.DEFAULT_DB);
    AnalysisError("SHOW GRANT ROLE myRole", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("SHOW GRANT ROLE myRole ON SERVER", authDisabledAnalyzer,
        "Authorization is not enabled.");
  }

  @Test
  public void AnalyzeCreateDropRole() throws AnalysisException {
    AnalysisError("DROP ROLE myRole", "Sentry Service is not supported on CDH4");
    AnalysisError("CREATE ROLE doesNotExist", "Sentry Service is not supported on CDH4");

    AnalysisError("DROP ROLE doesNotExist",  "Sentry Service is not supported on CDH4");
    AnalysisError("CREATE ROLE myRole",  "Sentry Service is not supported on CDH4");

    // Role names are case-insensitive
    AnalysisError("DROP ROLE MYrole", "Sentry Service is not supported on CDH4");
    AnalysisError("CREATE ROLE MYrole", "Sentry Service is not supported on CDH4");

    Analyzer authDisabledAnalyzer = createAuthDisabledAnalyzer(Catalog.DEFAULT_DB);
    AnalysisError("DROP ROLE myRole", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("CREATE ROLE doesNotExist", authDisabledAnalyzer,
        "Authorization is not enabled.");
  }

  @Test
  public void AnalyzeGrantRevokeRole() throws AnalysisException {
    AnalysisError("GRANT ROLE myrole TO GROUP abc",
        "Sentry Service is not supported on CDH4");
    AnalysisError("REVOKE ROLE myrole FROM GROUP abc",
        "Sentry Service is not supported on CDH4");
    AnalysisError("GRANT ROLE doesNotExist TO GROUP abc",
        "Sentry Service is not supported on CDH4");
    AnalysisError("REVOKE ROLE doesNotExist FROM GROUP abc",
        "Sentry Service is not supported on CDH4");

    Analyzer authDisabledAnalyzer = createAuthDisabledAnalyzer(Catalog.DEFAULT_DB);
    AnalysisError("GRANT ROLE myrole TO GROUP abc", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("REVOKE ROLE myrole FROM GROUP abc", authDisabledAnalyzer,
        "Authorization is not enabled.");
  }

  @Test
  public void AnalyzeGrantRevokePriv() throws AnalysisException {
    boolean[] isGrantVals = {true, false};
    for (boolean isGrant: isGrantVals) {
      Object[] formatArgs = new String[] {"REVOKE", "FROM"};
      if (isGrant) formatArgs = new String[] {"GRANT", "TO"};
      // ALL privileges
      AnalysisError(String.format("%s ALL ON TABLE foo %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s ALL ON TABLE bar.foo %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s ALL ON DATABASE foo %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s ALL ON SERVER %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s ALL ON URI 'hdfs:////abc//123' %s myrole",
          formatArgs), "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s ALL ON URI 'xxxx:////abc//123' %s myrole",
          formatArgs), "Sentry Service is not supported on CDH4");

      // INSERT privilege
      AnalysisError(String.format("%s INSERT ON TABLE foo %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s INSERT ON TABLE bar.foo %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s INSERT ON DATABASE foo %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s INSERT ON SERVER %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s INSERT ON URI 'hdfs:////abc//123' %s myrole",
          formatArgs), "Sentry Service is not supported on CDH4");

      AnalysisError(String.format("%s SELECT ON TABLE foo %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s SELECT ON TABLE bar.foo %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s SELECT ON DATABASE foo %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s SELECT ON TABLE foo %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s SELECT ON TABLE bar.foo %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s SELECT ON SERVER %s myrole", formatArgs),
          "Sentry Service is not supported on CDH4");
      AnalysisError(String.format("%s SELECT ON URI 'hdfs:////abc//123' %s myrole",
          formatArgs), "Sentry Service is not supported on CDH4");
    }

    Analyzer authDisabledAnalyzer = createAuthDisabledAnalyzer(Catalog.DEFAULT_DB);
    AnalysisError("GRANT ALL ON SERVER TO myRole", authDisabledAnalyzer,
        "Authorization is not enabled.");
    AnalysisError("REVOKE ALL ON SERVER FROM myRole", authDisabledAnalyzer,
        "Authorization is not enabled.");

    TQueryCtx queryCtxNoUsername = TestUtils.createQueryContext("default", "");
    Analyzer noUsernameAnalyzer = new Analyzer(catalog_, queryCtxNoUsername,
        AuthorizationConfig.createHadoopGroupAuthConfig("server1", null, null));
    AnalysisError("GRANT ALL ON SERVER TO myRole", noUsernameAnalyzer,
        "Sentry Service is not supported on CDH4");
  }
}

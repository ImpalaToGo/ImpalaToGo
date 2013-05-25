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

package org.apache.hive.service.cli.operation;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HiveDriverFilterHook;
import org.apache.hadoop.hive.ql.HiveDriverFilterHookContext;
import org.apache.hadoop.hive.ql.HiveDriverFilterHookContextImpl;
import org.apache.hadoop.hive.ql.HiveDriverFilterHookResult;
import org.apache.hadoop.hive.ql.hooks.Hook;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

/**
 * MetadataOperation.
 *
 */
public abstract class MetadataOperation extends Operation {

  protected static final String DEFAULT_HIVE_CATALOG = "";
  protected static TableSchema RESULT_SET_SCHEMA;
  private static final char SEARCH_STRING_ESCAPE = '\\';

  protected MetadataOperation(HiveSession parentSession, OperationType opType) {
    super(parentSession, opType);
    setHasResultSet(true);
  }


  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#close()
   */
  @Override
  public void close() throws HiveSQLException {
    setState(OperationState.CLOSED);
  }

  /**
   * Convert wildchars and escape sequence from JDBC format to datanucleous/regex
   */
  protected String convertIdentifierPattern(final String pattern, boolean datanucleusFormat) {
    if (pattern == null) {
      return convertPattern("%", true);
    } else {
      return convertPattern(pattern, datanucleusFormat);
    }
  }

  /**
   * Convert wildchars and escape sequence of schema pattern from JDBC format to datanucleous/regex
   * The schema pattern treats empty string also as wildchar
   */
  protected String convertSchemaPattern(final String pattern) {
    if ((pattern == null) || pattern.isEmpty()) {
      return convertPattern("%", true);
    } else {
      return convertPattern(pattern, true);
    }
  }

  /**
   * Convert a pattern containing JDBC catalog search wildcards into
   * Java regex patterns.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or
   * these characters escaped using {@link #getSearchStringEscape()}.
   * @return replace %/_ with regex search characters, also handle escaped
   * characters.
   *
   * The datanucleus module expects the wildchar as '*'. The columns search on the
   * other hand is done locally inside the hive code and that requires the regex wildchar
   * format '.*'  This is driven by the datanucleusFormat flag.
   */
  private String convertPattern(final String pattern, boolean datanucleusFormat) {
      String wStr;
      if (datanucleusFormat) {
        wStr = "*";
      } else {
        wStr = ".*";
      }
      return pattern
          .replaceAll("([^\\\\])%", "$1" + wStr).replaceAll("\\\\%", "%").replaceAll("^%", wStr)
          .replaceAll("([^\\\\])_", "$1.").replaceAll("\\\\_", "_").replaceAll("^_", ".");
  }

  private <T extends Hook> List<T> getHooks(HiveConf.ConfVars hookConfVar, Class<T> clazz)
      throws Exception {
    HiveConf conf = getParentSession().getHiveConf();
    List<T> hooks = new ArrayList<T>();
    String csHooks = conf.getVar(hookConfVar);
    if (csHooks == null) {
      return hooks;
    }

    csHooks = csHooks.trim();
    if (csHooks.equals("")) {
      return hooks;
    }

    String[] hookClasses = csHooks.split(",");

    for (String hookClass : hookClasses) {
      try {
        T hook =
            (T) Class.forName(hookClass.trim(), true, JavaUtils.getClassLoader()).newInstance();
        hooks.add(hook);
      } catch (ClassNotFoundException e) {
        LOG.error(hookConfVar.varname + " Class not found:" + e.getMessage());
        throw e;
      }
    }
    return hooks;
  }

  protected List<String> filterResultSet(List<String> inputResultSet, HiveOperation hiveOperation, String dbName)
    throws Exception {
    List<String> filteredResultSet = new ArrayList<String>();
    HiveConf conf = getParentSession().getHiveConf();
    String userName = getParentSession().getUserName();
    List<HiveDriverFilterHook> filterHooks = null;

    try {
      filterHooks = getHooks(HiveConf.ConfVars.HIVE_EXEC_FILTER_HOOK,
          HiveDriverFilterHook.class);
    } catch (Exception e) {
      LOG.error("Failed to obtain filter hooks");
      LOG.error(StringUtils.stringifyException(e));
    }

    // if the result set is non null, non empty and exec filter hooks are present
    // invoke the hooks to filter the result set
    if (inputResultSet != null && !inputResultSet.isEmpty() && filterHooks != null && !filterHooks.isEmpty() )  {
      HiveDriverFilterHookContext hookCtx = new HiveDriverFilterHookContextImpl(conf,
                                                           hiveOperation, userName, inputResultSet, dbName);
      HiveDriverFilterHookResult hookResult = null;
      for (HiveDriverFilterHook hook : filterHooks) {
        // result set 'inputResultSet' is passed to the filter hooks. The filter hooks shouldn't
        // mutate inputResultSet directly. They should return a filtered result set instead.
        hookResult = hook.postDriverFetch(hookCtx);
        ((HiveDriverFilterHookContextImpl)hookCtx).setResult(hookResult.getResult());
      }
      filteredResultSet.addAll(hookResult.getResult());
      return filteredResultSet;
    } else {
      return inputResultSet;
    }
  }
}

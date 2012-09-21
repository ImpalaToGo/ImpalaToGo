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

import org.apache.hive.service.sql.thrift.TGetInfoType;

/**
 * GetInfoType.
 *
 */
public enum GetInfoType {
  SQL_MAX_DRIVER_CONNECTIONS(TGetInfoType.SQL_MAX_DRIVER_CONNECTIONS),
  SQL_MAX_CONCURRENT_ACTIVITIES(TGetInfoType.SQL_MAX_CONCURRENT_ACTIVITIES),
  SQL_DATA_SOURCE_NAME(TGetInfoType.SQL_DATA_SOURCE_NAME),
  SQL_FETCH_DIRECTION(TGetInfoType.SQL_FETCH_DIRECTION),
  SQL_SERVER_NAME(TGetInfoType.SQL_SERVER_NAME),
  SQL_SEARCH_PATTERN_ESCAPE(TGetInfoType.SQL_SEARCH_PATTERN_ESCAPE),
  SQL_DBMS_NAME(TGetInfoType.SQL_DBMS_NAME),
  SQL_DBMS_VER(TGetInfoType.SQL_DBMS_VER),
  SQL_ACCESSIBLE_TABLES(TGetInfoType.SQL_ACCESSIBLE_TABLES),
  SQL_ACCESSIBLE_PROCEDURES(TGetInfoType.SQL_ACCESSIBLE_PROCEDURES),
  SQL_CURSOR_COMMIT_BEHAVIOR(TGetInfoType.SQL_CURSOR_COMMIT_BEHAVIOR),
  SQL_DATA_SOURCE_READ_ONLY(TGetInfoType.SQL_DATA_SOURCE_READ_ONLY),
  SQL_DEFAULT_TXN_ISOLATION(TGetInfoType.SQL_DEFAULT_TXN_ISOLATION),
  SQL_IDENTIFIER_CASE(TGetInfoType.SQL_IDENTIFIER_CASE),
  SQL_IDENTIFIER_QUOTE_CHAR(TGetInfoType.SQL_IDENTIFIER_QUOTE_CHAR),
  SQL_MAX_COLUMN_NAME_LEN(TGetInfoType.SQL_MAX_COLUMN_NAME_LEN),
  SQL_MAX_CURSOR_NAME_LEN(TGetInfoType.SQL_MAX_CURSOR_NAME_LEN),
  SQL_MAX_SCHEMA_NAME_LEN(TGetInfoType.SQL_MAX_SCHEMA_NAME_LEN),
  SQL_MAX_CATALOG_NAME_LEN(TGetInfoType.SQL_MAX_CATALOG_NAME_LEN),
  SQL_MAX_TABLE_NAME_LEN(TGetInfoType.SQL_MAX_TABLE_NAME_LEN),
  SQL_SCROLL_CONCURRENCY(TGetInfoType.SQL_SCROLL_CONCURRENCY),
  SQL_TXN_CAPABLE(TGetInfoType.SQL_TXN_CAPABLE),
  SQL_USER_NAME(TGetInfoType.SQL_USER_NAME),
  SQL_TXN_ISOLATION_OPTION(TGetInfoType.SQL_TXN_ISOLATION_OPTION),
  SQL_INTEGRITY(TGetInfoType.SQL_INTEGRITY),
  SQL_GETDATA_EXTENSIONS(TGetInfoType.SQL_GETDATA_EXTENSIONS),
  SQL_NULL_COLLATION(TGetInfoType.SQL_NULL_COLLATION),
  SQL_ALTER_TABLE(TGetInfoType.SQL_ALTER_TABLE),
  SQL_ORDER_BY_COLUMNS_IN_SELECT(TGetInfoType.SQL_ORDER_BY_COLUMNS_IN_SELECT),
  SQL_SPECIAL_CHARACTERS(TGetInfoType.SQL_SPECIAL_CHARACTERS),
  SQL_MAX_COLUMNS_IN_GROUP_BY(TGetInfoType.SQL_MAX_COLUMNS_IN_GROUP_BY),
  SQL_MAX_COLUMNS_IN_INDEX(TGetInfoType.SQL_MAX_COLUMNS_IN_INDEX),
  SQL_MAX_COLUMNS_IN_ORDER_BY(TGetInfoType.SQL_MAX_COLUMNS_IN_ORDER_BY),
  SQL_MAX_COLUMNS_IN_SELECT(TGetInfoType.SQL_MAX_COLUMNS_IN_SELECT),
  SQL_MAX_COLUMNS_IN_TABLE(TGetInfoType.SQL_MAX_COLUMNS_IN_TABLE),
  SQL_MAX_INDEX_SIZE(TGetInfoType.SQL_MAX_INDEX_SIZE),
  SQL_MAX_ROW_SIZE(TGetInfoType.SQL_MAX_ROW_SIZE),
  SQL_MAX_STATEMENT_LEN(TGetInfoType.SQL_MAX_STATEMENT_LEN),
  SQL_MAX_TABLES_IN_SELECT(TGetInfoType.SQL_MAX_TABLES_IN_SELECT),
  SQL_MAX_USER_NAME_LEN(TGetInfoType.SQL_MAX_USER_NAME_LEN),
  SQL_OJ_CAPABILITIES(TGetInfoType.SQL_OJ_CAPABILITIES),

  SQL_XOPEN_CLI_YEAR(TGetInfoType.SQL_XOPEN_CLI_YEAR),
  SQL_CURSOR_SENSITIVITY(TGetInfoType.SQL_CURSOR_SENSITIVITY),
  SQL_DESCRIBE_PARAMETER(TGetInfoType.SQL_DESCRIBE_PARAMETER),
  SQL_CATALOG_NAME(TGetInfoType.SQL_CATALOG_NAME),
  SQL_COLLATION_SEQ(TGetInfoType.SQL_COLLATION_SEQ),
  SQL_MAX_IDENTIFIER_LEN(TGetInfoType.SQL_MAX_IDENTIFIER_LEN);

  private final TGetInfoType tInfoType;

  GetInfoType(TGetInfoType tInfoType) {
    this.tInfoType = tInfoType;
  }

  public static GetInfoType getGetInfoType(TGetInfoType tGetInfoType) {
    // FIXME
    return SQL_CATALOG_NAME;
  }

  public TGetInfoType toTGetInfoType() {
    return tInfoType;
  }

}

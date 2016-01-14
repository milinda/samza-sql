/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.sql.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

public class SamzaSQLResultSetMetaData implements ResultSetMetaData {

  private final int columnCount;
  private final Map<Integer, String> indexToNameMap;

  public SamzaSQLResultSetMetaData(int columnCount, Map<Integer, String> indexToNameMap) {
    this.columnCount = columnCount;
    this.indexToNameMap = indexToNameMap;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columnCount;
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return 0;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return false;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return 128;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return indexToNameMap.get(column);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return indexToNameMap.get(column);
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return "";
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return 0;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return "";
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return "";
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return null;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}

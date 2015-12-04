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

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver {

  private final static String CONNECTION_URL_PREFIX = "jdbc:samzasql:";

  static {
    new Driver().register();
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    System.out.println("SamzaSQL JDBC driver connect called with following properties:");
    for(Map.Entry<Object,Object> entry : info.entrySet()) {
      System.out.println(String.format("\t%s:%s",entry.getKey(), entry.getValue()));
    }
    return null;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(CONNECTION_URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }

  private void register() {
    try {
      DriverManager.registerDriver(this);
    } catch (SQLException e) {
      System.out.println(
          "Error occurred while registering JDBC driver "
              + this + ": " + e.toString());
    }
  }
}

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

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

public class SamzaSQLResultSetRow {

  private Object[] row;

  private Map<String, Integer> columnNameToIndexMap;

  public SamzaSQLResultSetRow(Object[] row, Map<String, Integer> columnNameToIndexMap) {
    this.row = row;
    this.columnNameToIndexMap = columnNameToIndexMap;
  }

  public String getString(int columnIndex) throws SQLException {
    return (String)row[columnIndex];
  }

  public boolean getBoolean(int columnIndex) throws SQLException {
    return (Boolean)row[columnIndex];
  }

  public byte getByte(int columnIndex) throws SQLException {
    return (Byte)row[columnIndex];
  }

  public short getShort(int columnIndex) throws SQLException {
    return (Short)row[columnIndex];
  }

  public int getInt(int columnIndex) throws SQLException {
    return (Integer)row[columnIndex];
  }

  public long getLong(int columnIndex) throws SQLException {
    return (Long)row[columnIndex];
  }

  public float getFloat(int columnIndex) throws SQLException {
    return (Float)row[columnIndex];
  }

  public double getDouble(int columnIndex) throws SQLException {
    return (Double)row[columnIndex];
  }

  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return (BigDecimal)row[columnIndex];
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    return new byte[0];
  }

  public Date getDate(int columnIndex) throws SQLException {
    return (Date)row[columnIndex];
  }

  public Time getTime(int columnIndex) throws SQLException {
    return (Time)row[columnIndex];
  }

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return null;
  }

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return null;
  }

  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return null;
  }

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return null;
  }

  public String getString(String columnLabel) throws SQLException {
    return null;
  }

  public boolean getBoolean(String columnLabel) throws SQLException {
    return false;
  }

  public byte getByte(String columnLabel) throws SQLException {
    return 0;
  }

  public short getShort(String columnLabel) throws SQLException {
    return 0;
  }

  public int getInt(String columnLabel) throws SQLException {
    return 0;
  }

  public long getLong(String columnLabel) throws SQLException {
    return 0;
  }

  public float getFloat(String columnLabel) throws SQLException {
    return 0;
  }

  public double getDouble(String columnLabel) throws SQLException {
    return 0;
  }

  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return null;
  }

  public byte[] getBytes(String columnLabel) throws SQLException {
    return new byte[0];
  }

  public Date getDate(String columnLabel) throws SQLException {
    return null;
  }

  public Time getTime(String columnLabel) throws SQLException {
    return null;
  }

  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return null;
  }

  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return null;
  }

  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return null;
  }

  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return null;
  }
}

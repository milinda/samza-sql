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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.samza.sql.QueryExecutor;
import org.apache.samza.sql.api.Closeable;
import org.apache.samza.sql.jdbc.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class SamzaSQLConnectionImpl implements Connection, SamzaSQLConnection {
  private static final Logger log = LoggerFactory.getLogger(SamzaSQLConnectionImpl.class);

  public static final String PROP_MODEL = "model";
  public static final String PROP_ZOOKEEPER = "zookeeper";
  public static final String PROP_KAFKA = "kafka";

  private final List<Closeable> closeables = new ArrayList<Closeable>();

  private final Properties properties;

  private final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

  private String schema;

  private final QueryExecutor queryExecutor;

  private SamzaSQLMetaData connectionMetaData;

  public SamzaSQLConnectionImpl(Properties properties) throws SQLException, IOException {
    validateProps(properties);
    this.properties = properties;

    if (log.isDebugEnabled()) {
      log.debug("Connection properties: \n" + Utils.getPropertyAsString(properties));
    }

    String modelUri = properties.getProperty(PROP_MODEL);
    new ModelHandler(this, modelUri);

    String zookeeperConnectionStr = properties.getProperty(PROP_ZOOKEEPER);
    String kafkaBrokerList = properties.getProperty(PROP_KAFKA);
    this.queryExecutor = new QueryExecutor(this, zookeeperConnectionStr, kafkaBrokerList);
    this.connectionMetaData = new SamzaSQLMetaData(this);
  }

  private void validateProps(Properties properties) throws SQLException {
    if (!properties.containsKey(PROP_MODEL)
        || properties.getProperty(PROP_MODEL) == null
        || properties.getProperty(PROP_MODEL).isEmpty()) {
      throw new SQLException("Missing model JSON file.");
    }

    if (!properties.containsKey(PROP_ZOOKEEPER)
        || properties.getProperty(PROP_ZOOKEEPER) == null
        || properties.getProperty(PROP_ZOOKEEPER).isEmpty()) {
      throw new SQLException("Missing zookeeper connection string.");
    }

    if (!properties.containsKey(PROP_KAFKA)
        || properties.getProperty(PROP_KAFKA) == null
        || properties.getProperty(PROP_KAFKA).isEmpty()) {
      throw new SQLException("Missing Kafka broker list.");
    }
  }

  @Override
  public Statement createStatement() throws SQLException {
    System.out.println("Creating statement");
    if (log.isDebugEnabled()) {
      log.debug("Creating SamzaSQLStatement with properties " + Utils.getPropertyAsString(properties));
    }

    return new SamzaSQLStatement(queryExecutor);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    System.out.println("Preparing statement");
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return null;
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return null;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {

  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public void commit() throws SQLException {

  }

  @Override
  public void rollback() throws SQLException {

  }

  @Override
  public void close() throws SQLException {
    for (Closeable closeable : closeables) {
      closeable.close();
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return connectionMetaData;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {

  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {

  }

  @Override
  public String getCatalog() throws SQLException {
    return schema;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {

  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return 0;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    System.out.println("Create statement with extra conf");
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    System.out.println("Prepare statement withj extra info");
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    return null;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return null;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

  }

  @Override
  public void setHoldability(int holdability) throws SQLException {

  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return null;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return null;
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {

  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {

  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return null;
  }

  @Override
  public Clob createClob() throws SQLException {
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return null;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return false;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {

  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {

  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return null;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return null;
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return null;
  }

  @Override
  public SchemaPlus getRootSchema() {
    return rootSchema;
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return null;
  }

  @Override
  public Properties getProperties() {
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    this.schema = schema;
  }

  @Override
  public String getSchema() throws SQLException {
    return schema;
  }

  @Override
  public CalciteConnectionConfig config() {
    return null;
  }

  @Override
  public void abort(Executor executor) throws SQLException {

  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public <T> Queryable<T> createQuery(Expression expression, Class<T> rowType) {
    return null;
  }

  @Override
  public <T> Queryable<T> createQuery(Expression expression, Type rowType) {
    return null;
  }

  @Override
  public <T> T execute(Expression expression, Class<T> type) {
    return null;
  }

  @Override
  public <T> T execute(Expression expression, Type type) {
    return null;
  }

  @Override
  public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
    return null;
  }

  @Override
  public void registerCloseable(Closeable closeable) {
    closeables.add(closeable);
  }
}

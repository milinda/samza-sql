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

package org.apache.samza.sql;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.samza.job.JobRunner;
import org.apache.samza.sql.api.Closeable;
import org.apache.samza.sql.calcite.schema.SamzaSQLSchema;
import org.apache.samza.sql.jdbc.SamzaSQLConnection;
import org.apache.samza.sql.physical.JobConfigGenerator;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.planner.QueryPlannerContext;
import org.apache.samza.sql.planner.physical.SamzaRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class QueryExecutor implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);

  private final SamzaSQLConnection connection;
  private final QueryMetadataStore metadataStore;
  private final String kafkaBrokers;

  public QueryExecutor(SamzaSQLConnection connection, String zkConnectionString, String kafkaBrokers) throws IOException, SQLException {
    this.connection = connection;
    this.metadataStore = new QueryMetadataStore(zkConnectionString);
    this.kafkaBrokers = kafkaBrokers;

    // Register to listen to connection close event for cleaning up resources allocated.
    connection.registerCloseable(this);
  }

  private SchemaPlus getDefaultSchema() throws SQLException {
    return this.connection.getRootSchema().getSubSchema(connection.getSchema());
  }

  private SchemaPlus getRootSchema() {
    return this.connection.getRootSchema();
  }

  public void executeQuery(String query) throws Exception {
    String queryId = metadataStore.registerQuery(query);
    JobConfigGenerator jobConfigGenerator = new JobConfigGenerator(queryId, metadataStore);

    SchemaPlus defaultSchema = getDefaultSchema();
    SamzaSQLSchema samzaSQLSchema = defaultSchema.unwrap(SamzaSQLSchema.class);

    if (samzaSQLSchema == null) {
      throw new Exception(
          String.format("Default schema %s for this connection is not a SamzaSQLSchema instance.",
              connection.getSchema()));
    }

    // TODO: Only registering default schema will not work when we have multiple schemas pointing to
    // multiple Kafka clusters
    jobConfigGenerator.addKafkaSystem(defaultSchema.getName(),
        samzaSQLSchema.getZkConnectionString(),
        samzaSQLSchema.getBrokersList());

    QueryPlanner planner = new QueryPlanner(
        new QueryPlannerContextImpl(defaultSchema,
            getRootSchema()));
    SamzaRel queryPlan = planner.getPlan(query);

    defaultJobProps(jobConfigGenerator);
    jobConfigGenerator.setJobName(queryId);

    // TODO: Add zookeeper, kafka information to this job config
    queryPlan.populateJobConfiguration(jobConfigGenerator);

    // TODO: Save queries and configuraiotns somewhere (may be in zookeeper or in a dot file)
    Path jobPropsParent = Paths.get(System.getProperty("user.dir"), "query-configuraitons");
    Files.createDirectories(jobPropsParent);
    writeJobConfigToFile(jobPropsParent, queryId, jobConfigGenerator.getJobConfig());

    new SamzaSQLJobRunner(jobConfigGenerator.getJobConfig()).run();
  }

  private void defaultJobProps(JobConfigGenerator jobConfigGenerator) {
    jobConfigGenerator.setJobFactory(JobConfigGenerator.YARN_JOB_FACTORY);
    jobConfigGenerator.setTaskCheckpointFactory(JobConfigGenerator.KAFKA_CHECKPOINT_FACTORY);
  }

  private Path writeJobConfigToFile(Path parentDir, String queryId, Map<String, String> propsMap) throws IOException {
    Path propFilePath = parentDir.resolve(String.format("%s.properties", queryId));
    Files.createFile(propFilePath);

    Properties props = new Properties();
    props.putAll(propsMap);
    props.store(new FileWriter(propFilePath.toFile()), null);

    return propFilePath;
  }

  public class QueryPlannerContextImpl implements QueryPlannerContext {

    private final SchemaPlus defaultSchema;

    private final SchemaPlus rootSchema;

    public QueryPlannerContextImpl(SchemaPlus defaultSchema, SchemaPlus rootSchema) {
      this.defaultSchema = defaultSchema;
      this.rootSchema = rootSchema;
    }

    @Override
    public SchemaPlus getDefaultSchema() {
      return defaultSchema;
    }

    @Override
    public SqlOperatorTable getSamzaOperatorTable() {
      return SqlStdOperatorTable.instance();
    }
  }

  public void close() {
    this.metadataStore.close();
  }
}

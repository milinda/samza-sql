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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.job.StreamJob;
import org.apache.samza.sql.api.Closeable;
import org.apache.samza.sql.api.metastore.SamzaSQLMetaStore;
import org.apache.samza.sql.jdbc.SamzaSQLConnection;
import org.apache.samza.sql.physical.JobConfigGenerator;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.planner.physical.SamzaRel;
import org.apache.samza.sql.schema.SamzaSQLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class QueryExecutor implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);

  private final SamzaSQLConnection connection;
  private final SamzaSQLMetaStore metadataStore;
  private final Config config;

  public QueryExecutor(SamzaSQLConnection connection, Config config) throws IOException, SQLException {
    this.config = config;
    this.connection = connection;
    this.metadataStore = SamzaSQLUtils.getMetaStoreFactoryInstance(config).createMetaStore(config);

    // Register to listen to connection close event for cleaning up resources allocated.
    connection.registerCloseable(this);
  }

  private SchemaPlus getDefaultSchema() throws SQLException {
    return getRootSchema().getSubSchema(connection.getSchema());
  }

  private SchemaPlus getRootSchema() {
    return this.connection.getRootSchema();
  }

  public StreamJob executeQuery(String query) throws Exception {
    String queryId = metadataStore.registerQuery(query);
    JobConfigGenerator jobConfigGenerator = new JobConfigGenerator(queryId, metadataStore);

    SchemaPlus defaultSchema = getDefaultSchema();

    if(!isSamzaSQLSchema(defaultSchema)){
      throw new SamzaException("Default schema is not an instance of SamzaSQLSchema");
    }

    addSystems(jobConfigGenerator, getRootSchema());
    jobConfigGenerator.setModel(metadataStore.registerCalciteModel(queryId, connection.getModel()));

    QueryPlanner planner = new QueryPlanner(defaultSchema);
    SamzaRel queryPlan = planner.getPlan(query);

    defaultJobProps(jobConfigGenerator, defaultSchema.getName());
    jobConfigGenerator.setJobName(queryId);

    queryPlan.populateJobConfiguration(jobConfigGenerator);

    return new SamzaSQLJobRunner(jobConfigGenerator.getJobConfig()).run(true);
  }

  private void defaultJobProps(JobConfigGenerator jobConfigGenerator, String defaultSchemaName) {
    jobConfigGenerator.setJobFactory(JobConfigGenerator.THREAD_JOB_FACTORY);
    jobConfigGenerator.setTaskClass(SamzaSQLTask.class.getName());
    //jobConfigGenerator.setTaskCheckpointFactory(JobConfigGenerator.KAFKA_CHECKPOINT_FACTORY);
    jobConfigGenerator.setCoordinatorSystem(defaultSchemaName); // TODO: Fix this and use a separate system
    jobConfigGenerator.setCoordinatorReplicationFactor(1);
    jobConfigGenerator.setMetadataStoreFactory("org.apache.samza.sql.metastore.ZKBakedQueryMetaStoreFactory");
    jobConfigGenerator.setMetaStoreZKConnectionString(config.get("samza.sql.metastore.zk.connect"));
  }

  private boolean isSamzaSQLSchema(SchemaPlus schema) {
    CalciteSchema calciteSchema = schema.unwrap(CalciteSchema.class);
    return calciteSchema.schema instanceof SamzaSQLSchema;
  }

  private void addSystems(JobConfigGenerator jobConfigGenerator, SchemaPlus root) throws Exception {
    for (String subSchemaName : root.getSubSchemaNames()) {
      SchemaPlus subSchema = root.getSubSchema(subSchemaName);
      CalciteSchema calciteSchema = subSchema.unwrap(CalciteSchema.class);
      if (calciteSchema != null) {
        if (calciteSchema.schema instanceof SamzaSQLSchema) {
          SamzaSQLSchema schema = (SamzaSQLSchema) calciteSchema.schema;
          if(schema.getBrokersList() != null && schema.getZkConnectionString() != null) {
            jobConfigGenerator.addKafkaSystem(subSchemaName, schema.getZkConnectionString(), schema.getBrokersList());
          }
        }
      }
    }
  }

  public void close() {
    this.metadataStore.close();
  }
}

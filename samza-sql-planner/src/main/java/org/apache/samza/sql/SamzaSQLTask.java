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

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.metastore.SamzaSQLMetaStore;
import org.apache.samza.sql.api.metastore.SamzaSQLMetaStoreFactory;
import org.apache.samza.sql.api.operators.OperatorRouter;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.jdbc.SamzaSQLConnection;
import org.apache.samza.sql.physical.JobConfigurations;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.planner.SamzaSQLConnectionImpl;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;

public class SamzaSQLTask implements StreamTask, InitableTask {

  private OperatorRouter operatorRouter;

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    String queryId = config.get(JobConfigurations.JOB_NAME);
    SamzaSQLMetaStoreFactory metaStoreFactory = SamzaSQLUtils.getMetaStoreFactoryInstance(config);
    SamzaSQLMetaStore metaStore = metaStoreFactory.createMetaStore(config);
    String calciteModel = metaStore.getCalciteModel(queryId, config.get(JobConfigurations.CALCITE_MODEL));
    String query = metaStore.getQuery(queryId);

    SamzaSQLConnection connection = new SamzaSQLConnectionImpl(calciteModel);
    QueryPlanner queryPlanner = new QueryPlanner(connection.getRootSchema().getSubSchema(connection.getSchema()));
    this.operatorRouter = queryPlanner.getOperatorRouter(query);
    this.operatorRouter.init(config, context);
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    this.operatorRouter.process(new IncomingMessageTuple(envelope), collector, coordinator);
  }
}

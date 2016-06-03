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

package org.apache.samza.sql.bench;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.bench.join.JoinOperator;
import org.apache.samza.sql.bench.join.ProductsStreamScanOperator;
import org.apache.samza.sql.bench.join.ProjectAfterJoinOperator;
import org.apache.samza.sql.bench.slidingwindow.OrdersStreamScanOperator;
import org.apache.samza.sql.bench.utils.OutputOperator;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.SimpleRouter;
import org.apache.samza.sql.operators.join.JoinSpec;
import org.apache.samza.sql.operators.join.JoinType;
import org.apache.samza.sql.physical.insert.InsertToStreamSpec;
import org.apache.samza.sql.physical.project.ProjectSpec;
import org.apache.samza.sql.physical.scan.RelationScanSpec;
import org.apache.samza.sql.physical.scan.StreamScanSpec;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;

public class JoinSQLTask implements StreamTask, InitableTask {

  final RelProtoDataType protoJoinOutputRowType = new RelProtoDataType() {
    @Override
    public RelDataType apply(RelDataTypeFactory a0) {
      return a0.builder()
          .add("rowtime", SqlTypeName.TIMESTAMP)
          .add("orderId", SqlTypeName.INTEGER)
          .add("productId", SqlTypeName.INTEGER)
          .add("units", SqlTypeName.INTEGER)
          .add("supplierId", SqlTypeName.INTEGER)
          .build();
    }
  };

  private final SimpleRouter router = new SimpleRouter();

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    final EntityName ordersScanOutput = EntityName.getIntermediateStream();
    final EntityName productsScanOutput = EntityName.getIntermediateTable();
    final EntityName joinOutput = EntityName.getIntermediateStream();
    final EntityName projectOutput = EntityName.getIntermediateStream();

    router.addOperator(new OrdersStreamScanOperator(new StreamScanSpec("ordersscan", EntityName.getStreamName("kafka:orders"), ordersScanOutput)));
    router.addOperator(new ProductsStreamScanOperator(new RelationScanSpec("productsscan", EntityName.getStreamName("kafka:products"), productsScanOutput, "operation")));
    router.addOperator(new JoinOperator(new JoinSpec("join", ordersScanOutput, productsScanOutput, joinOutput, JoinType.INNER, null), productsScanOutput));
    router.addOperator(new ProjectAfterJoinOperator(new ProjectSpec("projectafterjoin", joinOutput, projectOutput, null)));
    router.addOperator(new OutputOperator(new InsertToStreamSpec("joinoutput", projectOutput, EntityName.getStreamName("kafka:joinsqloutput")), protoJoinOutputRowType, "joinsqloutput"));
    router.init(config, context);
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    router.process(new IncomingMessageTuple(envelope), collector, coordinator);
  }
}

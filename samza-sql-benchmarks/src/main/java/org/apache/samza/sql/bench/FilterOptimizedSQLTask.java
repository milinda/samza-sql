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
import org.apache.samza.sql.bench.filter.FilterOperator;
import org.apache.samza.sql.bench.filter.ScanAndFilterOperator;
import org.apache.samza.sql.bench.slidingwindow.OrdersStreamScanOperator;
import org.apache.samza.sql.bench.utils.OutputOperator;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.SimpleRouter;
import org.apache.samza.sql.physical.filter.FilterSpec;
import org.apache.samza.sql.physical.insert.InsertToStreamSpec;
import org.apache.samza.sql.physical.scan.StreamScanSpec;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;

public class FilterOptimizedSQLTask implements StreamTask, InitableTask {
  final RelProtoDataType filterOutputType = new RelProtoDataType() {
    public RelDataType apply(RelDataTypeFactory a0) {
      return a0.builder()
          .add("orderId", SqlTypeName.INTEGER)
          .add("productId", SqlTypeName.INTEGER)
          .add("units", SqlTypeName.INTEGER)
          .add("rowtime", SqlTypeName.TIMESTAMP)
          .add("padding", SqlTypeName.VARCHAR, 128)
          .build();
    }
  };

  private final SimpleRouter router = new SimpleRouter();

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    EntityName scanOutput = EntityName.getIntermediateStream();
    router.addOperator(new ScanAndFilterOperator(new FilterSpec("ordersscan", EntityName.getStreamName("kafka:orders"), scanOutput, null)));
    router.addOperator(new OutputOperator(new InsertToStreamSpec("filterorders", scanOutput, EntityName.getStreamName("kafka:filteroptimizedsqloutput")), filterOutputType, "filteroptimizedsqloutput"));
    router.init(config, context);
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    router.process(new IncomingMessageTuple(envelope), collector, coordinator);
  }
}

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

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.bench.slidingwindow.*;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.SimpleRouter;
import org.apache.samza.sql.physical.insert.InsertToStreamSpec;
import org.apache.samza.sql.physical.project.ProjectSpec;
import org.apache.samza.sql.physical.scan.StreamScanSpec;
import org.apache.samza.sql.physical.window.WindowOperatorSpec;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;

public class SlidingWindowSQLStreamTask implements StreamTask, InitableTask {
  private final SimpleRouter router = new SimpleRouter();

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    EntityName scanOutput = EntityName.getIntermediateStream();
    EntityName firstProjectOutput = EntityName.getIntermediateStream();
    EntityName windowOutput = EntityName.getIntermediateStream();
    EntityName secondProjectOutput = EntityName.getIntermediateStream();
    EntityName finalOutput = EntityName.getStreamName("kafka:slidingwindowout");
    router.addOperator(new OrdersStreamScanOperator(new StreamScanSpec("ordersscan", EntityName.getStreamName("kafka:orders"), scanOutput)));
    router.addOperator(new FirstProjectOperator(new ProjectSpec("firstproject", scanOutput, firstProjectOutput, null)));
    router.addOperator(SlidingWindowSumOperatorFactory.create(new WindowOperatorSpec("slidingwindowsub", firstProjectOutput, windowOutput)));
    router.addOperator(new ProjectAfterWindowOperator(new ProjectSpec("secondproject", windowOutput, secondProjectOutput, null)));
    router.addOperator(new WriteSlidingWindowResultsOperator(new InsertToStreamSpec("writeslidingsum", secondProjectOutput, finalOutput)));
    router.init(config, context);
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    router.process(new IncomingMessageTuple(envelope), collector, coordinator);
  }
}

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

package org.apache.samza.sql.bench.project;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Schema;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.bench.utils.ProjectProductIdAndUnitsExpression;
import org.apache.samza.sql.data.DataUtils;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.data.TupleConverter;
import org.apache.samza.sql.operators.SimpleOperatorImpl;
import org.apache.samza.sql.physical.project.ProjectSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

public class ScanAndProjectOperator extends SimpleOperatorImpl {
  final RelProtoDataType protoProjectType = new RelProtoDataType() {
    public RelDataType apply(RelDataTypeFactory a0) {
      return a0.builder()
          .add("productId", SqlTypeName.INTEGER)
          .add("units", SqlTypeName.INTEGER)
          .add("rowtime", SqlTypeName.TIMESTAMP)
          .build();
    }
  };



  final RelProtoDataType protoScanType = new RelProtoDataType() {
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


  private final RelDataType projectType;
  private final RelDataType scanType;
  private final ProjectSpec spec;

  public ScanAndProjectOperator(ProjectSpec spec) {
    super(spec);
    this.spec = spec;
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    this.projectType = protoProjectType.apply(typeFactory);
    this.scanType = protoScanType.apply(typeFactory);
  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Tuple tuple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if(!DataUtils.isStruct(tuple.getMessage())) {
      throw new SamzaException(String.format("Unsupported tuple type: %s expected: %s", tuple.getMessage().schema().getType(), Schema.Type.STRUCT));
    }

    Object[] inputMsg = TupleConverter.samzaDataToObjectArray(tuple.getMessage(), scanType);


    Object[] output = new Object[projectType.getFieldCount()];
    new ProjectProductIdAndUnitsExpression().execute(inputMsg, output);

    collector.send(IntermediateMessageTuple.fromData(output, tuple.getKey(),
        tuple.getCreateTimeNano(), tuple.getOffset(), tuple.isDelete(), spec.getOutputName()));
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {

  }
}

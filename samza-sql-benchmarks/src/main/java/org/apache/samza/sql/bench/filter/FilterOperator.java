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

package org.apache.samza.sql.bench.filter;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.expr.Expression;
import org.apache.samza.sql.operators.SimpleOperatorImpl;
import org.apache.samza.sql.physical.filter.FilterSpec;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

public class FilterOperator extends SimpleOperatorImpl {
  final RelProtoDataType protoRowType = new RelProtoDataType() {
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

  private final FilterSpec filterSpec;
  private final RelDataType type;

  public FilterOperator(FilterSpec spec) {
    super(spec);
    this.filterSpec = spec;
    this.type = protoRowType.apply(new JavaTypeFactoryImpl());
  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Tuple tuple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if(!(tuple instanceof IntermediateMessageTuple )) {
      throw new SamzaException("Unsupported insput tuple type: " + tuple.getClass());
    }

    Object[] inputMsg = ((IntermediateMessageTuple)tuple).getContent();

    Expression filter = new Expression() {
      @Override
      public Object execute(Object[] inputValues) {
        if((Integer)inputValues[2] > 25) {
          return true;
        }
        return false;
      }

      @Override
      public void execute(Object[] inputValues, Object[] results) {

      }
    };

    Boolean condition = (Boolean) filter.execute(inputMsg);

    if(condition) {
      collector.send(IntermediateMessageTuple.fromData(inputMsg, tuple.getKey(),
          tuple.getCreateTimeNano(), tuple.getOffset(), tuple.isDelete(), filterSpec.getOutputName()));
    }
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {

  }
}

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
package org.apache.samza.sql.test;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Schema;
import org.apache.samza.sql.data.avro.AvroSchema;
import org.apache.samza.sql.schema.AvroSchemaUtils;
import org.apache.samza.sql.schema.SamzaStreamType;
import org.apache.samza.sql.schema.Stream;

import java.util.Map;

public class ProjectedOrdersStreamFactory implements TableFactory<Table> {

  public Table create(SchemaPlus schema, String name,
                      Map<String, Object> operand, RelDataType rowType) {
    final RelProtoDataType protoRowType = new RelProtoDataType() {
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder()
            .add("productId", SqlTypeName.VARCHAR, 10)
            .add("quantity", SqlTypeName.INTEGER)
            .build();
      }
    };

    final SamzaStreamType streamType = new SamzaStreamType() {
      @Override
      public Schema apply(RelDataTypeFactory a0) {
        return AvroSchema.getSchema(AvroSchemaUtils.relDataTypeToAvroSchema(protoRowType.apply(a0)));
      }
    };

    return new Stream(EntityName.getStreamName(String.format("%s:%s", "kafka", "projectedorders")),
        streamType) {

      @Override
      public Table stream() {
        return null;
      }

      @Override
      public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new LogicalTableScan(context.getCluster(), relOptTable);
      }

      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return protoRowType.apply(typeFactory);
      }

      @Override
      public Statistic getStatistic() {
        return Statistics.UNKNOWN;
      }
    };
  }

}

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
package org.apache.samza.sql.planner.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.physical.JobConfigGenerator;
import org.apache.samza.sql.physical.JobConfigurations;
import org.apache.samza.sql.physical.PhysicalPlanCreator;
import org.apache.samza.sql.physical.insert.InsertToStream;
import org.apache.samza.sql.physical.insert.InsertToStreamSpec;
import org.apache.samza.sql.planner.common.SamzaStreamInsertRelBase;
import org.apache.samza.sql.schema.SamzaSQLStream;
import org.apache.samza.sql.schema.SamzaSQLTable;
import org.apache.samza.sql.utils.IdGenerator;

import java.util.List;

public class SamzaStreamInsertRel extends SamzaStreamInsertRelBase implements SamzaRel {
  public SamzaStreamInsertRel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table,
                              Prepare.CatalogReader catalogReader, RelNode child,
                              TableModify.Operation operation, List<String> updateColumnList,
                              boolean flattened) {
    super(cluster, traits, table, catalogReader, child, operation, updateColumnList, flattened);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SamzaStreamInsertRel(getCluster(), traitSet,  getTable(), getCatalogReader(), sole(inputs),
        getOperation(), getUpdateColumnList(), isFlattened());
  }

  @Override
  public void populateJobConfiguration(JobConfigGenerator configGenerator) throws Exception {
    ((SamzaRel)getInput()).populateJobConfiguration(configGenerator);

    SamzaSQLStream samzaTable = table.unwrap(SamzaSQLStream.class);

    String msgSerdeName = String.format("%s-msgserde", samzaTable.getStreamName());
    if (samzaTable.getMessageSchemaType() == SamzaSQLTable.MessageSchemaType.AVRO) {
      configGenerator.addSerde(msgSerdeName, JobConfigGenerator.AVRO_SERDE_FACTORY);
      configGenerator.addConfig(
          String.format(JobConfigurations.SERIALIZERS_SCHEMA, msgSerdeName),
          configGenerator.getQueryMetaStore().registerMessageType(configGenerator.getQueryId(),
              samzaTable.getMessageSchema()));
    } else {
      throw new SamzaException("Only Avro message format is supported at this stage.");
    }

    String keySerdeName = SamzaRelUtils.deriveAndPopulateKeySerde(configGenerator, samzaTable);

    configGenerator.addStream(samzaTable.getSystem(), samzaTable.getStreamName(), keySerdeName, msgSerdeName, false);
  }

  @Override
  public void physicalPlan(PhysicalPlanCreator physicalPlanCreator) throws Exception {
    SamzaRel input = (SamzaRel)getInput();
    input.physicalPlan(physicalPlanCreator);
    OperatorSpec inputOpSpec = physicalPlanCreator.pop();
    SamzaSQLStream outputStream = table.unwrap(SamzaSQLStream.class);

    physicalPlanCreator.addOperator(new InsertToStream(
        new InsertToStreamSpec(IdGenerator.generateOperatorId("InsertToStream"),
            sole(inputOpSpec.getOutputNames()),
            EntityName.getStreamName(String.format("%s:%s", outputStream.getSystem(), outputStream.getStreamName()))),
        input.getRowType()
    ));
  }

  @Override
  public <T> T accept(SamzaRelVisitor<T> visitor) {
    return null;
  }
}

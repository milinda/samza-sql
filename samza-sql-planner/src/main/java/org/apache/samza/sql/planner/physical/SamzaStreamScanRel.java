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
import org.apache.samza.SamzaException;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.physical.JobConfigGenerator;
import org.apache.samza.sql.physical.JobConfigurations;
import org.apache.samza.sql.physical.PhysicalPlanCreator;
import org.apache.samza.sql.physical.scan.StreamScan;
import org.apache.samza.sql.physical.scan.StreamScanSpec;
import org.apache.samza.sql.planner.common.SamzaStreamScanRelBase;
import org.apache.samza.sql.schema.SamzaSQLStream;
import org.apache.samza.sql.schema.SamzaSQLTable;
import org.apache.samza.sql.utils.IdGenerator;

public class SamzaStreamScanRel extends SamzaStreamScanRelBase implements SamzaRel {

  public SamzaStreamScanRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, table);
  }

  @Override
  public void populateJobConfiguration(JobConfigGenerator configGenerator) throws Exception {
    SamzaSQLStream samzaTable = table.unwrap(SamzaSQLStream.class);

    // Note: If we are doing this in Calcite way we should use RelOptTable#names.
    // But not yet sure the names can be matched to Samza <system>:<stream> format
    // if there are more than two elements in RelOptTable#names().
    // TODO: Register serdes and schemas.

    String msgSerdeName = String.format("%s-messageserde", samzaTable.getStreamName());
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
    configGenerator.addInput(String.format("%s.%s", samzaTable.getSystem(), samzaTable.getStreamName()));
  }

  @Override
  public void physicalPlan(PhysicalPlanCreator physicalPlanCreator) throws Exception {
    SamzaSQLStream inputStream = table.unwrap(SamzaSQLStream.class);
    physicalPlanCreator.addOperator(
        new StreamScan(
            new StreamScanSpec(IdGenerator.generateOperatorId("StreamScan"),
                EntityName.getStreamName(String.format("%s:%s", inputStream.getSystem(), inputStream.getStreamName())),
                EntityName.getAnonymousStream()),
            getRowType()));
  }

  @Override
  public <T> T accept(SamzaRelVisitor<T> visitor) {
    return null;
  }
}

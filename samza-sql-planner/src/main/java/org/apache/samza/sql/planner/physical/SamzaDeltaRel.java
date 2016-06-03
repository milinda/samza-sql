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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.stream.Delta;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.physical.JobConfigGenerator;
import org.apache.samza.sql.physical.PhysicalPlanCreator;
import org.apache.samza.sql.physical.insert.InsertToStreamSpec;
import org.apache.samza.sql.utils.IdGenerator;

import java.util.List;
import java.util.UUID;

public class SamzaDeltaRel extends Delta implements SamzaRel {
  public SamzaDeltaRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SamzaDeltaRel(getCluster(), traitSet, sole(inputs));
  }

  @Override
  public void populateJobConfiguration(JobConfigGenerator configGenerator) throws Exception {
    ((SamzaRel)getInput()).populateJobConfiguration(configGenerator);
  }

  @Override
  public void physicalPlan(PhysicalPlanCreator physicalPlanCreator) throws Exception {
    ((SamzaRel)getInput()).physicalPlan(physicalPlanCreator);
    OperatorSpec inputOpSpec = physicalPlanCreator.pop();

    // Note: I decided to replace delta with insert-to-stream in the physical plan. Still not sure whether this is the correct approach.
    physicalPlanCreator.addOperator(
            new org.apache.samza.sql.physical.insert.InsertToStream(
                    new InsertToStreamSpec(IdGenerator.generateOperatorId("InsertToStream"),
                            sole(inputOpSpec.getOutputNames()),
                            EntityName.getStreamName(String.format("%s:%s", "kafka", UUID.randomUUID().toString()))),
                    getRowType()
            ));
  }

  @Override
  public <T> T accept(SamzaRelVisitor<T> visitor) {
    return null;
  }
}

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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.physical.JobConfigGenerator;
import org.apache.samza.sql.physical.PhysicalPlanCreator;
import org.apache.samza.sql.physical.project.ProjectSpec;
import org.apache.samza.sql.planner.common.SamzaProjectRelBase;
import org.apache.samza.sql.utils.IdGenerator;

import java.util.List;

public class SamzaProjectRel extends SamzaProjectRelBase implements SamzaRel {
  public SamzaProjectRel(RelOptCluster cluster, RelTraitSet traits, RelNode input,
                         List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traits, input, projects, rowType);
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new SamzaProjectRel(getCluster(), traitSet, input, projects, rowType);
  }

  @Override
  public void populateJobConfiguration(JobConfigGenerator configGenerator) throws Exception {
    ((SamzaRel)getInput()).populateJobConfiguration(configGenerator);
  }

  @Override
  public void physicalPlan(PhysicalPlanCreator physicalPlanCreator) throws Exception {
    ((SamzaRel)getInput()).physicalPlan(physicalPlanCreator);
    OperatorSpec inputSpec = physicalPlanCreator.pop();

    physicalPlanCreator.addOperator(
        new org.apache.samza.sql.physical.project.Project(
            new ProjectSpec(IdGenerator.generateOperatorId("Project"),
                sole(inputSpec.getOutputNames()),
                EntityName.getAnonymousStream(),
                physicalPlanCreator.compile(getInputs(), getProjects())),
            getRowType()
        )
    );
  }

  @Override
  public <T> T accept(SamzaRelVisitor<T> visitor) {
    return null;
  }
}

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
package org.apache.samza.sql.planner.logical;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.expr.RexToJavaCompiler;
import org.apache.samza.sql.physical.PhysicalPlanCreator;
import org.apache.samza.sql.physical.filter.FilterSpec;
import org.apache.samza.sql.planner.common.SamzaFilterRelBase;
import org.apache.samza.sql.utils.IdGenerator;

public class SamzaFilterRel extends SamzaFilterRelBase implements SamzaRel {
  public SamzaFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new SamzaFilterRel(getCluster(), traitSet, input, condition);
  }

  @Override
  public void physicalPlan(PhysicalPlanCreator physicalPlanCreator) throws Exception {
    ((SamzaRel)getInput()).physicalPlan(physicalPlanCreator);
    OperatorSpec inputOpSpec = physicalPlanCreator.pop();

    physicalPlanCreator.addOperator(
        new org.apache.samza.sql.physical.filter.Filter(
            new FilterSpec(IdGenerator.generateOperatorId("Filter"),
                sole(inputOpSpec.getOutputNames()),
                EntityName.getIntermediateStream(),
                physicalPlanCreator.compile(getInputs(), Lists.newArrayList(getCondition())))
        ));
  }

  @Override
  public <T> T accept(SamzaRelVisitor<T> visitor) {
    return null;
  }
}

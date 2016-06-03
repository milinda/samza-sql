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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.samza.sql.physical.JobConfigGenerator;
import org.apache.samza.sql.physical.PhysicalPlanCreator;
import org.apache.samza.sql.planner.common.SamzaJoinRelBase;

import java.util.Set;

public class SamzaJoinRel extends SamzaJoinRelBase implements SamzaRel {


  public SamzaJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys, JoinRelType joinType, Set<String> variablesStopped) {
    super(cluster, traits, left, right, condition, leftKeys, rightKeys, joinType, variablesStopped);
  }

  @Override
  public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right,
                   JoinRelType joinType, boolean semiJoinDone) {
    final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
    if(!joinInfo.isEqui()) {
      throw new AssertionError("Join should be a equi-join");
    }

    return new SamzaJoinRel(getCluster(), traitSet, left, right, conditionExpr, joinInfo.leftKeys,
        joinInfo.rightKeys, joinType, getVariablesStopped()); // TODO: Investigate how variablesStopped is retrieved and used here
  }

  @Override
  public void populateJobConfiguration(JobConfigGenerator configGenerator) throws Exception {
    ((SamzaRel)getLeft()).populateJobConfiguration(configGenerator);
    ((SamzaRel)getRight()).populateJobConfiguration(configGenerator);
    // TODO: We need to populate join specific config here.
  }

  @Override
  public void physicalPlan(PhysicalPlanCreator physicalPlanCreator) {

  }

  @Override
  public <T> T accept(SamzaRelVisitor<T> visitor) {
    return null;
  }
}

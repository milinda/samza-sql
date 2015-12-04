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
package org.apache.samza.sql.planner.physical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.samza.sql.planner.physical.SamzaJoinRel;
import org.apache.samza.sql.planner.physical.SamzaLogicalConvention;

public class SamzaJoinRule extends ConverterRule {
  public static final SamzaJoinRule INSTANCE = new SamzaJoinRule();

  private SamzaJoinRule() {
    super(LogicalJoin.class, Convention.NONE, SamzaLogicalConvention.INSTANCE, "SamzaJoinRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    final Join join = (Join) rel;
    final RelNode leftInput = join.getLeft();
    final RelNode rightInput = join.getRight();

    return new SamzaJoinRel(join.getCluster(),
        join.getTraitSet().replace(SamzaLogicalConvention.INSTANCE),
        convert(leftInput, leftInput.getTraitSet().replace(SamzaLogicalConvention.INSTANCE)),
        convert(rightInput, rightInput.getTraitSet().replace(SamzaLogicalConvention.INSTANCE)),
        join.getCondition(), join.getJoinType(), join.getVariablesStopped());
  }
}

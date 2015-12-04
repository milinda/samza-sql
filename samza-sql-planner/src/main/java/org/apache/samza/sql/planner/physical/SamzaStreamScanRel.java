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
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.physical.PhysicalPlanCreator;
import org.apache.samza.sql.physical.scan.StreamScan;
import org.apache.samza.sql.physical.scan.StreamScanSpec;
import org.apache.samza.sql.planner.common.SamzaStreamScanRelBase;
import org.apache.samza.sql.utils.IdGenerator;

public class SamzaStreamScanRel extends SamzaStreamScanRelBase implements SamzaRel {

  public SamzaStreamScanRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, table);
  }

  @Override
  public void physicalPlan(PhysicalPlanCreator physicalPlanCreator) throws Exception {
    physicalPlanCreator.addOperator(
        new StreamScan(
            new StreamScanSpec(IdGenerator.generateOperatorId("StreamScan"),
                samzaStream.getName(),
                EntityName.getIntermediateStream()),
            getRowType()));
  }

  @Override
  public <T> T accept(SamzaRelVisitor<T> visitor) {
    return null;
  }
}

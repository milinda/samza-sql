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
package org.apache.samza.sql.planner.logical.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.samza.sql.planner.logical.SamzaLogicalConvention;
import org.apache.samza.sql.planner.logical.SamzaWindowRel;

public class SamzaWindowRule extends ConverterRule {
  public static final SamzaWindowRule INSTANCE = new SamzaWindowRule();

  private SamzaWindowRule() {
    super(LogicalWindow.class, Convention.NONE, SamzaLogicalConvention.INSTANCE, "SamzaWindowRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    final Window window = (Window) rel;
    final RelNode input = window.getInput();

    return new SamzaWindowRel(window.getCluster(),
        window.getTraitSet().replace(SamzaLogicalConvention.INSTANCE),
        convert(input, input.getTraitSet().replace(SamzaLogicalConvention.INSTANCE)),
        window.constants,
        window.getRowType(),
        window.groups);
  }
}

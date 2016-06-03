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
package org.apache.samza.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.samza.sql.planner.physical.rules.*;

import java.util.Iterator;

public class SamzaRuleSets {
  /**
   * Converter rule set that converts from Calcite logical convention to Samza physical convention.
   */
  private static final ImmutableSet<RelOptRule> calciteToSamzaConversionRules =
      ImmutableSet.<RelOptRule>builder().add(
          SortRemoveRule.INSTANCE,
          AbstractConverter.ExpandConversionRule.INSTANCE,
          SamzaDeltaRule.INSTANCE,
          SamzaScanRule.INSTANCE,
          SamzaFilterRule.INSTANCE,
          SamzaProjectRule.INSTANCE,
          SamzaSortRule.INSTANCE,
          SamzaWindowRule.INSTANCE,
          SamzaAggregateRule.INSTANCE,
          SamzaJoinRule.INSTANCE,
          SamzaModifyRule.INSTANCE
      ).build();

  public static RuleSet[] getRuleSets() {
    /*
     * Calcite planner takes an array of RuleSet and we can refer to them by index to activate
     * each rule set for transforming the query plan based on different criteria.
     */
    return new RuleSet[]{new SamzaRuleSet(StreamRules.RULES), new SamzaRuleSet(ImmutableSet.<RelOptRule>builder().addAll(StreamRules.RULES).addAll(calciteToSamzaConversionRules).build())};
  }

  private static class SamzaRuleSet implements RuleSet {
    final ImmutableSet<RelOptRule> rules;

    public SamzaRuleSet(ImmutableSet<RelOptRule> rules) {
      this.rules = rules;
    }

    public SamzaRuleSet(ImmutableList<RelOptRule> rules) {
      this.rules = ImmutableSet.<RelOptRule>builder()
          .addAll(rules)
          .build();
    }

    @Override
    public Iterator<RelOptRule> iterator() {
      return rules.iterator();
    }
  }
}

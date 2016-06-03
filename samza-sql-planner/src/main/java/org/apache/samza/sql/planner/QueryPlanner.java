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


import com.google.common.collect.Lists;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.api.operators.OperatorRouter;
import org.apache.samza.sql.physical.PhysicalPlanCreator;
import org.apache.samza.sql.planner.physical.SamzaLogicalConvention;
import org.apache.samza.sql.planner.physical.SamzaRel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryPlanner {

  public static int LOGICAL_RULES = 0;

  private final Planner planner;
  private final HepPlanner hepPlanner;

  public QueryPlanner(QueryPlannerContext context) {
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.configBuilder()
            .setLex(Lex.MYSQL)
            .build())
        .defaultSchema(context.getDefaultSchema())                            // TODO: Get default schema
        .operatorTable(context.getSamzaOperatorTable())  // TODO: How to provide Samza specific operator table
        .traitDefs(traitDefs)
        .context(Contexts.EMPTY_CONTEXT)                // This can be used to store data within the planner session and access them within the rules.
        .ruleSets(SamzaRuleSets.getRuleSets())
        .costFactory(null)                              // Custom cost factory
        .typeSystem(SamzaRelDataTypeSystem.SAMZA_REL_DATATYPE_SYSTEM)
        .build();
    this.planner = Frameworks.getPlanner(config);
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleClass(ReduceExpressionsRule.class);
    hepProgramBuilder.addRuleClass(ProjectToWindowRule.class);
    this.hepPlanner = new HepPlanner(hepProgramBuilder.build());
    this.hepPlanner.addRule(ReduceExpressionsRule.CALC_INSTANCE);
    this.hepPlanner.addRule(ProjectToWindowRule.PROJECT);
  }

  public OperatorRouter getOperatorRouter(String query) throws Exception {
    SamzaRel relNode = getPlan(query);
    PhysicalPlanCreator physicalPlanCreator =
        PhysicalPlanCreator.create(relNode.getCluster().getTypeFactory());

    relNode.physicalPlan(physicalPlanCreator);

    return physicalPlanCreator.getRouter();
  }

  public SamzaRel getPlan(String query) throws ValidationException, RelConversionException {
    SqlNode sqlNode;

    try {
      sqlNode = planner.parse(query);
    } catch (SqlParseException e) {
      throw new SamzaException("Query parsing error.", e);
    }

    // TODO: We need to fix exception handling and also performs the conversion to Samza specific
    // RelNode implementations.

    return (SamzaRel) validateAndConvert(sqlNode);
  }

  private RelNode validateAndConvert(SqlNode sqlNode) throws ValidationException, RelConversionException {
    SqlNode validated = validateNode(sqlNode);
    RelNode relNode = convertToRelNode(validated);

    System.out.println(RelOptUtil.toString(relNode));

    // Drill does pre-processing too. We can do pre processing here if needed.
    // Drill also preserve the validated type of the sql query for later use. But current Calcite
    // API doesn't provide a way to get validated type.

    return convertToSamzaRel(relNode);
  }

  private RelNode convertToSamzaRel(RelNode relNode) throws RelConversionException {
    RelTraitSet traitSet = relNode.getTraitSet();
    traitSet = traitSet.simplify(); // TODO: Is this the correct thing to do? Why relnode has a composite trait?
    return planner.transform(LOGICAL_RULES, traitSet.plus(SamzaLogicalConvention.INSTANCE), relNode);
  }

  private RelNode convertToRelNode(SqlNode sqlNode) throws RelConversionException {
    final RelNode convertedNode = planner.convert(sqlNode);

    // Below code is fromData Drill. But exactly not sure what they are doing with metadata provider.
    // In out old code we could do the same window planning without messing with metadata provider.
    final RelMetadataProvider provider = convertedNode.getCluster().getMetadataProvider();

    // Register RelMetadataProvider with HepPlanner.
    final List<RelMetadataProvider> list = Lists.newArrayList(provider);
    hepPlanner.registerMetadataProviders(list);
    final RelMetadataProvider cachingMetaDataProvider = new CachingRelMetadataProvider(ChainedRelMetadataProvider.of(list), hepPlanner);
    convertedNode.accept(new MetaDataProviderModifier(cachingMetaDataProvider));

    // HepPlanner is specifically used for Window Function planning only. This is true for Samza
    // too.
    hepPlanner.setRoot(convertedNode);
    RelNode rel = hepPlanner.findBestExp();

    // I think this line reset the metadata provider instances changed for hep planner execution.
    rel.accept(new MetaDataProviderModifier(provider));

    return rel;
  }

  private SqlNode validateNode(SqlNode sqlNode) throws ValidationException {
    SqlNode validatedSqlNode = planner.validate(sqlNode);

    validatedSqlNode.accept(new UnsupportedOperatorsVisitor());

    return validatedSqlNode;
  }

  // TODO: This is fromData Drill. Not sure what it does.
  public static class MetaDataProviderModifier extends RelShuttleImpl {
    private final RelMetadataProvider metadataProvider;

    public MetaDataProviderModifier(RelMetadataProvider metadataProvider) {
      this.metadataProvider = metadataProvider;
    }

    @Override
    public RelNode visit(TableScan scan) {
      scan.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(scan);
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
      scan.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(scan);
    }

    @Override
    public RelNode visit(LogicalValues values) {
      values.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(values);
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      child.accept(this);
      parent.getCluster().setMetadataProvider(metadataProvider);
      return parent;
    }
  }

}

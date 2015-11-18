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
package org.apache.samza.sql.physical;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.api.operators.OperatorRouter;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.api.operators.SimpleOperator;
import org.apache.samza.sql.expr.Expression;
import org.apache.samza.sql.expr.RexToJavaCompiler;
import org.apache.samza.sql.operators.SimpleRouter;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Stack;

/**
 * TODO: Physical plan creator should keep names of local storage units that are used by operator. Also should keep
 * TODO: type information useful when configuring Serdes.
 */
public class PhysicalPlanCreator {
  private final OperatorRouter router;

  private final Deque<OperatorSpec> operatorStack = new ArrayDeque<OperatorSpec>();

  private final RexToJavaCompiler expressionCompiler;

  private final JavaTypeFactory typeFactory;

  private PhysicalPlanCreator(OperatorRouter router, RexBuilder rexBuilder){
    this.router = router;
    this.expressionCompiler = new RexToJavaCompiler(rexBuilder);
    this.typeFactory = (JavaTypeFactory)rexBuilder.getTypeFactory();
  }

  /**
   * Create an instance of {@link PhysicalPlanCreator}
   *
   * @param relDataTypeFactory The {@link RelDataTypeFactory} instance
   * @return an instance of {@link PhysicalPlanCreator}
   */
  public static final PhysicalPlanCreator create(RelDataTypeFactory relDataTypeFactory){
    if(!(relDataTypeFactory instanceof JavaTypeFactory)){
      throw new SamzaException("Type factory should be a JavaTypeFactory");
    }

    return new PhysicalPlanCreator(new SimpleRouter(), new RexBuilder(relDataTypeFactory));
  }

  /**
   * Add a new operator instance to physical plan.
   *
   * @param operator The {@link SimpleOperator} instance
   * @throws Exception
   */
  public void addOperator(SimpleOperator operator) throws Exception {
    // Push the operator spec to stack to use it with operators in upstream
    push(operator.getSpec());
    router.addOperator(operator);
  }

  /**
   * Get operator router representing this physical plan.
   *
   * @return {@link OperatorRouter} instnace
   */
  public OperatorRouter getRouter() {
    return router;
  }

  /**
   * Returns the last inserted item to {@link OperatorSpec} stack.
   *
   * @return last inserted {@link OperatorSpec} instance
   */
  public OperatorSpec pop(){
    return operatorStack.pop();
  }

  /**
   * Insert an {@link OperatorSpec} instance to the stack
   * @param spec The {@link OperatorSpec} instance
   */
  protected void push(OperatorSpec spec) {
    operatorStack.push(spec);
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  /**
   * Generate an implementation of {@link Expression} representing set of relational expressions,
   * compile it and creates an instance of the implementaiton.
   *
   * @param inputs Inputs to the relational expressions
   * @param expressions relational expressions
   * @return instance of generated expression implementation
   */
  public Expression compile(List<RelNode> inputs, List<RexNode> expressions) {
    return expressionCompiler.compile(inputs, expressions);
  }
}

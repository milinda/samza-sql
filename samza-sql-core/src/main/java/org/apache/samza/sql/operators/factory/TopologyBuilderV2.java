/**
 * Copyright (C) 2015 Trustees of Indiana University
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.samza.sql.operators.factory;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.expressions.ScalarExpression;
import org.apache.samza.sql.api.expressions.TupleExpression;
import org.apache.samza.sql.api.operators.*;
import org.apache.samza.sql.operators.SimpleRouter;
import org.apache.samza.sql.operators.filter.FilterSpec;
import org.apache.samza.sql.operators.join.JoinSpec;
import org.apache.samza.sql.operators.join.JoinType;
import org.apache.samza.sql.operators.modify.Operation;
import org.apache.samza.sql.operators.modify.StreamModifySpec;
import org.apache.samza.sql.operators.modify.TableModifySpec;
import org.apache.samza.sql.operators.partition.FieldBasedPartitionKeyGenerator;
import org.apache.samza.sql.operators.partition.PartitionSpec;
import org.apache.samza.sql.operators.project.ProjectSpec;
import org.apache.samza.sql.operators.scan.StreamScanSpec;
import org.apache.samza.sql.operators.scan.TableScanSpec;

import java.util.*;

/**
 * This class implements a fluent style builder to allow user to create the operators and
 * connect them in a topology altogether.
 */
public class TopologyBuilderV2 {

  /**
   * The {@link org.apache.samza.sql.api.operators.OperatorRouter} instance to retain the
   * topology being created.
   */
  private SimpleRouter router;

  /**
   * The {@link org.apache.samza.sql.api.operators.SqlOperatorFactory} instance used
   * to create operators
   * used in the topology from operator specs.
   */
  private SqlOperatorFactory operatorFactory;

  /**
   * Stack to keep track of necessary context information required to build the topology.
   */
  private Deque<OperatorSpec> stack = new ArrayDeque<OperatorSpec>();

  /**
   * Private constructor of {@code TopologyBuilderV2}
   *
   * @param operatorFactory The {@link org.apache.samza.sql.api.operators.SqlOperatorFactory}
   *                        to create operators
   */
  private TopologyBuilderV2(SqlOperatorFactory operatorFactory) {
    this.router = new SimpleRouter();
    this.operatorFactory = operatorFactory;
  }

  /**
   * Static method to create this {@code TopologyBuilderV2} w/ a customized
   * {@link org.apache.samza.sql.api.operators.SqlOperatorFactory}
   *
   * @param operatorFactory The {@link org.apache.samza.sql.api.operators.SqlOperatorFactory} to
   *                        create operators
   * @return The {@code TopologyBuilderV2} object
   */
  public static TopologyBuilderV2 create(SqlOperatorFactory operatorFactory) {
    return new TopologyBuilderV2(operatorFactory);
  }

  /**
   * Static method to create this {@code TopologyBuilderV2}
   *
   * @return The {@code TopologyBuilderV2} object
   */
  public static TopologyBuilderV2 create() {
    return new TopologyBuilderV2(new SimpleOperatorFactoryImpl());
  }

  /**
   * Private method to add a operator to the router. Ordering of the operator being added will be
   * decided based on input/output streams specified in the operator spec.
   *
   * @param operatorSpec The {@link org.apache.samza.sql.api.operators.OperatorSpec} for the
   *                     operator being added
   */
  private void addRoutes(OperatorSpec operatorSpec) {
    SimpleOperator operator = operatorFactory.getOperator(operatorSpec);
    router.addOperator(operator);
  }

  /**
   * Private method to add a operator to the router. Ordering of the operator being added will be
   * decided based on input/output streams specified in the operator spec.
   *
   * @param operator The {@link org.apache.samza.sql.api.operators.Operator} being added
   */
  private void addRoutes(SimpleOperator operator) {
    router.addOperator(operator);
  }

  /**
   * Add a scan operator to the physical query plan.
   *
   * @param source A stream or a table to scan
   * @return The updated {@code TopologyBuilderV2} object
   */
  public TopologyBuilderV2 scan(EntityName source) {
    if (isInputOperatorAModification()) {
      throw new IllegalStateException(
          "scan is not allowed here, because previous operator was a modify.");
    }

    OperatorSpec scanSpec;
    String operatorId = generateOperatorId();
    if (source.isStream()) {
      scanSpec = new StreamScanSpec(operatorId, source, EntityName.getIntermediateStream());
    } else {
      // TODO: Investigate this further? I don't think we have   to scan and emit table content to a stream.
      scanSpec = new TableScanSpec(operatorId, source, EntityName.getIntermediateTable());
    }

    stack.push(scanSpec);
    addRoutes(scanSpec);

    return this;
  }

  /**
   * Add a scan operator to the physical query plan.
   *
   * @param view A view to scan
   * @return The updated {@code TopologyBuilderV2} object
   */
  public TopologyBuilderV2 scan(View view) {
    if (isInputOperatorAModification()) {
      throw new IllegalStateException(
          "scan is not allowed here, because previous operator was a modify.");
    }

    Iterator<SimpleOperator> operators = view.iterator();

    while (operators.hasNext()) {
      addRoutes(operators.next());
    }

    OperatorSpec lastOpSpec = view.lastOperatorSpec();

    if(lastOpSpec == null) {
      throw new IllegalArgumentException(
          String.format("View %s is not compatible due to unavailability of an operator with " +
              "unbounded output.", view.getName()));
    }

    stack.push(lastOpSpec);

    return this;
  }

  /**
   * Add a filter operator to the physical query plan.
   *
   * @param expression A instance of  an {@link ScalarExpression}
   *                   encapsulating the filter condition
   * @return The updated {@code TopologyBuilderV2} object
   */
  public TopologyBuilderV2 filter(ScalarExpression expression) {
    if (isInputOperatorAModification()) {
      throw new IllegalStateException(
          "filter is not allowed here, because previous operator was a modify.");
    }

    OperatorSpec inputOperatorSpec = stack.pop();
    OperatorSpec filterOperatorSpec = new FilterSpec(generateOperatorId(),
        inputOperatorSpec.getOutputNames().get(0),
        EntityName.getIntermediateStream(),
        expression);

    stack.push(filterOperatorSpec);
    addRoutes(filterOperatorSpec);

    return this;
  }

  /**
   * Add a project operator to the physical query plan.
   *
   * @param expression A instance of  an {@link ScalarExpression}
   *                   encapsulating the projection
   * @return The updated {@code TopologyBuilderV2} object
   */
  public TopologyBuilderV2 project(TupleExpression expression) {
    if (isInputOperatorAModification()) {
      throw new IllegalStateException(
          "project is not allowed here, because previous operator was a modify.");
    }

    OperatorSpec inputOperatorSpec = stack.pop();
    OperatorSpec projectOperatorSpec = new ProjectSpec(generateOperatorId(),
        inputOperatorSpec.getOutputNames().get(0),
        EntityName.getIntermediateStream(),
        expression);

    stack.push(projectOperatorSpec);
    addRoutes(projectOperatorSpec);

    return this;
  }

  /**
   * Add a join operator to the physical query plan.
   *
   * @param joinType      Join type
   * @param joinCondition A instance of  an {@link ScalarExpression}
   *                      encapsulating the join condition
   * @return The updated {@code TopologyBuilderV2} object
   */
  public TopologyBuilderV2 join(JoinType joinType, ScalarExpression joinCondition) {
    if (isInputOperatorAModification()) {
      throw new IllegalStateException(
          "join is not allowed here, because previous operator was a modify.");
    }

    OperatorSpec rightInput = stack.pop();
    OperatorSpec leftInput = stack.pop();
    OperatorSpec joinSpec = new JoinSpec(generateOperatorId(),
        leftInput.getOutputNames().get(0), rightInput.getOutputNames().get(0),
        EntityName.getIntermediateStream(), joinType, joinCondition);

    stack.push(joinSpec);
    addRoutes(joinSpec);

    return this;
  }

  /**
   * Add a modify operator to the physical query plan.
   *
   * @param operation Modify operation
   * @param output    Entity being modified by this operator
   * @return The updated {@code TopologyBuilderV2} object
   */
  public TopologyBuilderV2 modify(Operation operation, EntityName output) {
    if (isInputOperatorAModification()) {
      throw new IllegalStateException(
          "modify is not allowed here, because previous operator was a modify.");
    }

    OperatorSpec inputOperatorSpec = stack.pop();
    OperatorSpec modifySpec;
    if (output.isStream()) {
      modifySpec = new StreamModifySpec(generateOperatorId(),
          operation,
          inputOperatorSpec.getOutputNames().get(0),
          output);
    } else {
      modifySpec = new TableModifySpec(generateOperatorId(),
          operation,
          inputOperatorSpec.getOutputNames().get(0),
          output);
    }

    stack.push(modifySpec);
    addRoutes(modifySpec);

    return this;
  }

  /**
   * Add a partition operator to the physical query plan.
   *
   * @param numPartitions     number of partitions to create
   * @param partitionByFields fields to use for generating partition key
   * @return The updated {@code TopologyBuilderV2} object
   */
  public TopologyBuilderV2 partition(EntityName output, int numPartitions, String... partitionByFields) {
    if (isInputOperatorAModification()) {
      throw new IllegalStateException(
          "partition is not allowed here, because previous operator was a modify.");
    }

    OperatorSpec inputOperatorSpec = stack.pop();
    OperatorSpec partitionSpec = new PartitionSpec(generateOperatorId(),
        inputOperatorSpec.getOutputNames().get(0), output, numPartitions,
        new FieldBasedPartitionKeyGenerator(partitionByFields));
    stack.push(partitionSpec);
    addRoutes(partitionSpec);

    return this;
  }

  /**
   * Add a partition operator to the physical query plan.
   *
   * @param numPartitions number of partitions to create
   * @param partitionExpr expression to use for generating partition key
   * @return The updated {@code TopologyBuilderV2} object
   */
  public TopologyBuilderV2 partition(EntityName output, int numPartitions,
                                     ScalarExpression partitionExpr) {
    if (isInputOperatorAModification()) {
      throw new IllegalStateException(
          "partition is not allowed here, because previous operator was a modify.");
    }

    OperatorSpec inputOperatorSpec = stack.pop();
    OperatorSpec partitionSpec = new PartitionSpec(generateOperatorId(),
        inputOperatorSpec.getOutputNames().get(0), output, numPartitions, partitionExpr);
    stack.push(partitionSpec);
    addRoutes(partitionSpec);

    return this;
  }


  public TopologyBuilderV2 window() {
    return this;
  }

  public TopologyBuilderV2 aggregate() {
    return this;
  }

  private boolean isInputOperatorAModification() {
    OperatorSpec input = stack.peek();

    return (input instanceof StreamModifySpec || input instanceof TableModifySpec);
  }

  public OperatorRouter buildQuery() {
    return resetRouterAndGetOld();
  }

  private String generateOperatorId() {
    // TODO: Implement a proper operator id generation logic. Id generation factory?
    return UUID.randomUUID().toString();
  }

  public View buildView(String name) {
    return new View(name, resetRouterAndGetOld());
  }

  public View buildView() {
    return new View("anonymous-view-" + UUID.randomUUID().toString(), resetRouterAndGetOld());
  }

  private SimpleRouter resetRouterAndGetOld(){
    SimpleRouter current = this.router;
    this.router = new SimpleRouter();

    return current;
  }


  public static class View {
    private final SimpleRouter router;
    private final String name;

    public View(String name, SimpleRouter router) {
      this.name = name;
      this.router = router;
    }

    public Iterator<SimpleOperator> iterator() {
      return this.router.iterator();
    }

    public String getName() {
      return name;
    }

    public OperatorSpec lastOperatorSpec() {
      Map<EntityName, OperatorSpec> unboundedOutputs = new HashMap<EntityName, OperatorSpec>();
      Iterator<SimpleOperator> operators = router.iterator();
      OperatorSpec lastOpSpec = null;

      while (operators.hasNext()) {
        SimpleOperator operator = operators.next();
        OperatorSpec spec = operator.getSpec();

        // Operator with an unbounded output is the last operator
        if (!isBoundedOutput(spec.getOutputNames().get(0))) {
          return spec;
        }
      }

      return null;
    }

    /**
     * Check whether a output entity is an input to an another operator.
     *
     * @param entityName The output entity name.
     * @return true if output is bound, false otherwise
     */
    private boolean isBoundedOutput(EntityName entityName) {
      Iterator<SimpleOperator> operators = router.iterator();

      while (operators.hasNext()) {
        SimpleOperator operator = operators.next();

        for (EntityName input : operator.getSpec().getInputNames()) {
          if (input.equals(entityName)) {
            return true;
          }
        }
      }

      return false;
    }
  }


}

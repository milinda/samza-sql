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

package org.apache.samza.sql.physical.window.codegen;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.codegen.SamzaBuiltInMethod;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.physical.aggregate.*;
import org.apache.samza.sql.window.storage.TimeAndOffsetKey;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generates the code for a window operator based on {@link org.apache.samza.sql.planner.logical.SamzaWindowRel}.
 * <p/>
 * Logic
 * -----
 * <p/>
 * Idea is to use a template (abstract class)
 */
public class WindowOperatorGenerator {

  private final JavaTypeFactory typeFactory;
  private final BlockBuilder blockBuilder;
  private static AtomicInteger windowOperatorCounter = new AtomicInteger(0);

  public WindowOperatorGenerator(JavaTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
    this.blockBuilder = new BlockBuilder();
  }

  public WindowOperator generate(Window windowRel) {
    final List<Expression> translatedConstants = new ArrayList<Expression>(windowRel.constants.size());
    for (RexLiteral constant : windowRel.constants) {
      translatedConstants.add(RexToLixTranslator.translateLiteral(constant,
          constant.getType(), typeFactory, RexImpTable.NullAs.NULL));
    }

    PhysType inputType = PhysTypeImpl.of(typeFactory, windowRel.getInput().getRowType(), JavaRowFormat.ARRAY);
    PhysType outputType = PhysTypeImpl.of(typeFactory, windowRel.getRowType(), JavaRowFormat.ARRAY);

    final ParameterExpression tuple =
        Expressions.parameter(Tuple.class, "tuple");
    final ParameterExpression collector =
        Expressions.parameter(SimpleMessageCollector.class, "collector");
    final ParameterExpression coordinator =
        Expressions.parameter(TaskCoordinator.class, "coordinator");

    ParameterExpression intTuple = Expressions.parameter(IntermediateMessageTuple.class, "intTuple");
    blockBuilder.add(Expressions.declare(0, intTuple, Expressions.convert_(tuple, IntermediateMessageTuple.class)));

    RexToLixTranslator.InputGetter inputGetter = new WindowRelInputGetter(
        Expressions.call(intTuple, SamzaBuiltInMethod.INTERMEDIATE_TUPLE_GET_CONTENT.method),
        inputType,
        inputType.getRowType().getFieldCount(), translatedConstants);

    int i = 0;
    for (Window.Group group : windowRel.groups) {
      List<StreamingAggImpState> aggs = new ArrayList<StreamingAggImpState>();
      List<AggregateCall> aggregateCalls = group.getAggregateCalls(windowRel);
      for (int aggIdx = 0; aggIdx < aggregateCalls.size(); aggIdx++) {
        AggregateCall call = aggregateCalls.get(aggIdx);
        aggs.add(new StreamingAggImpState(aggIdx, call, true));
      }

      // Initialize WindowStore instance
      blockBuilder.add(Expressions.ifThen(
          Expressions.equal(
              Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
                  SamzaBuiltInMethod.WINDOWOP_GET_WINDOW_STORE.method, Expressions.constant(i)),
              Expressions.constant(null)),
          generateInitWindowStore(i)));

      // Initialize MessageStore instance
      blockBuilder.add(Expressions.ifThen(
          Expressions.equal(
              Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
                  SamzaBuiltInMethod.WINDOWOP_GET_MESSAGE_STORE.method, Expressions.constant(i)),
              Expressions.constant(null)),
          generateInitMessageStore(i)));

      final RexToLixTranslator translator = RexToLixTranslator.forAggregation(typeFactory, blockBuilder, inputGetter);

      // Initialize GroupState instance
      blockBuilder.add(Expressions.ifThen(
          Expressions.equal(
              Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
                  SamzaBuiltInMethod.WINDOWOP_GET_GROUP_STATE.method, Expressions.constant(i)),
              Expressions.constant(null)),
          generateInitGroupState(i, 0L) // TODO: Find out how range works
      ));

      // TODO: it is possible to use Group.isRows to figure out whether this is a row based window
      // Extract timestamp
      // First validating the collation
      if (group.collation().getFieldCollations().size() != 1 ||
          !isValidOrderByFieldType(group.collation().getFieldCollations().get(0), windowRel.getInput().getRowType())) {
        throw new SamzaException("Doesn't support order by multiple fields.");
      }

      // Extract timestamp and store it
      RelFieldCollation fieldCollation = group.collation().getFieldCollations().get(0);
      ParameterExpression tupleTimestamp = Expressions.parameter(Long.class, "tupleTimestamp");
      blockBuilder.add(Expressions.declare(0, tupleTimestamp, null));


      blockBuilder.add(Expressions.statement(Expressions.assign(tupleTimestamp,
          inputGetter.field(blockBuilder,
              fieldCollation.getFieldIndex(),
              typeFactory.getJavaClass(getFieldType(fieldCollation.getFieldIndex(), windowRel.getInput().getRowType()))))));

      // Detect replay
      blockBuilder.add(Expressions.ifThen(
          Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
              SamzaBuiltInMethod.WINDOWOP_IS_REPLAY.method, Expressions.constant(i), tupleTimestamp, intTuple),
          Expressions.return_(null)));

      // Calculate lower bound
      // if(tupleTimestamp > getUpperBound(i)) {
      //   updateUpperBound(i, tupleTimestamp);
      // }
      //
      // if (getLowerBound(i) == Long.MAX_VALUE) {
      //   updateLowerBound(i, tupleTimestamp);
      // } else {
      //   Long diff = calculateLowerBound(getUpperBound(i));
      //   if ( diff > 0 ) {
      //     updateLowerBound(i, diff);
      //   }
      // }

      final ParameterExpression newLowerBound = Expressions.parameter(Long.class, "newLowerBound");

      // Update upper bound
      blockBuilder.add(Expressions.ifThen(
          Expressions.greaterThan(
              tupleTimestamp,
              Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
                  SamzaBuiltInMethod.WINDOWOP_GET_UPPER_BOUND.method, Expressions.constant(i))),
          Expressions.statement(Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
              SamzaBuiltInMethod.WINDOWOP_UPDATE_UPPER_BOUND.method, Expressions.constant(i), tupleTimestamp))
      ));

      // Update lower bound
      BlockBuilder updateLowerBoundBlock = new BlockBuilder();
      updateLowerBoundBlock.add(Expressions.declare(0, newLowerBound,
          translateLowerBoundUpdate(group.lowerBound, translator, group.collation().getFieldCollations(), inputType)));
      updateLowerBoundBlock.add(Expressions.ifThen(
          Expressions.greaterThan(newLowerBound, Expressions.constant(0)),
          Expressions.statement(Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
              SamzaBuiltInMethod.WINDOWOP_UPDATE_LOWER_BOUND.method, Expressions.constant(i), newLowerBound))
      ));

      blockBuilder.add(Expressions.ifThenElse(
          Expressions.equal(Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
              SamzaBuiltInMethod.WINDOWOP_GET_LOWER_BOUND.method, Expressions.constant(i)), Expressions.constant(Long.MAX_VALUE)),
          Expressions.statement(Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
              SamzaBuiltInMethod.WINDOWOP_UPDATE_LOWER_BOUND.method, Expressions.constant(i), tupleTimestamp)),
          updateLowerBoundBlock.toBlock()));

      // Add the message offset to the current window. If we want the actual message we can retrieve it from message store.
      blockBuilder.add(Expressions.statement(
          Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
              SamzaBuiltInMethod.WINDOWOP_ADD_MESSAGE.method, Expressions.constant(i), tupleTimestamp, intTuple)));

      blockBuilder.add(Expressions.ifThen(Expressions.equal(
          Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
              SamzaBuiltInMethod.WINDOWOP_GET_PARTITIONS.method, Expressions.constant(i)), Expressions.constant(null)),
          Expressions.statement(Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
              SamzaBuiltInMethod.WINDOWOP_INIT_PARTITIONS.method, Expressions.constant(i)))));

      ParameterExpression aggregateAdjuster = Expressions.parameter(0, Function2.class, "aggregateAdjuster");
      blockBuilder.add(Expressions.declare(0, aggregateAdjuster, generateAdjustAggregate(group, i, aggs, inputType, translatedConstants, windowRel.constants)));

      blockBuilder.add(Expressions.statement(
          Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
              SamzaBuiltInMethod.WINDOWOP_PURGE_MESSAGES.method,
              Expressions.constant(i), aggregateAdjuster)));

      // Update aggregate with new tuple and emit results
      generateUpdateAndEmit(blockBuilder, group, i, String.valueOf(i), aggs, inputType, outputType, inputGetter,
          Expressions.call(intTuple, SamzaBuiltInMethod.INTERMEDIATE_TUPLE_GET_CONTENT.method), intTuple,
          translatedConstants, windowRel.constants, collector);

      // Save message in message store
      blockBuilder.add(Expressions.statement(
          Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
              SamzaBuiltInMethod.WINDOWOP_ADD_TO_MESSAGE_STORE.method,
              Expressions.constant(i),
              Expressions.new_(TimeAndOffsetKey.class, tupleTimestamp,
                  Expressions.call(intTuple, SamzaBuiltInMethod.INTERMEDIATE_TUPLE_GET_OFFSET.method)),
              intTuple)));

      i++;
    }


    return genClass(blockBuilder.toBlock(), tuple, collector, coordinator);
  }

  private void derivePartitionKey(BlockBuilder block, ParameterExpression partitionKey, Window.Group group,
                                  PhysType inputRowType, String pKeyBuilderSuffix,
                                  RexToLixTranslator.InputGetter inputGetter) {
    ParameterExpression partitionKeyBuilder =
        Expressions.parameter(WindowOperator.PartitionKeyBuilder.class, "partitionKeyBuilder" + pKeyBuilderSuffix);

    if (group.keys.isEmpty()) {
      // If partition keys are empty we will only have a single partition
      block.add(Expressions.declare(0, partitionKeyBuilder,
          Expressions.new_(WindowOperator.PartitionKeyBuilder.class, Expressions.constant(1))));
      block.add(Expressions.statement(
          Expressions.call(partitionKeyBuilder, SamzaBuiltInMethod.PKEY_BUILDER_SET.method, Expressions.constant(0), Expressions.constant("NOKEY"))));
      block.add(Expressions.declare(0, partitionKey, Expressions.call(partitionKeyBuilder, SamzaBuiltInMethod.PKEY_BUILDER_BUILD.method)));
    } else {
      block.add(Expressions.declare(0, partitionKeyBuilder,
          Expressions.new_(WindowOperator.PartitionKeyBuilder.class, Expressions.constant(group.keys.size()))));
      int j = 0;
      for (Integer field : group.keys.toList()) {
        block.add(Expressions.statement(
            Expressions.call(partitionKeyBuilder,
                SamzaBuiltInMethod.PKEY_BUILDER_SET.method,
                Expressions.constant(j),
                inputGetter.field(
                    block,
                    field,
                    typeFactory.getJavaClass(getFieldType(field, inputRowType.getRowType()))))));
      }
      block.add(Expressions.declare(0, partitionKey, Expressions.call(partitionKeyBuilder, SamzaBuiltInMethod.PKEY_BUILDER_BUILD.method)));
    }
  }

  private void getAndInitAggregateStateIfNull(BlockBuilder parent, ParameterExpression aggregateState,
                                              ParameterExpression partitionKey, Expression partitions,
                                              List<StreamingAggImpState> aggs) {
    parent.add(Expressions.declare(0, aggregateState, Expressions.convert_(
        Expressions.call(partitions, SamzaBuiltInMethod.MAP_GET.method, partitionKey),
        WindowOperator.AggregateState.class)));

    // If aggregate state is null, initialize it with default values
    final BlockBuilder initAggState = new BlockBuilder(true);

    initAggState.add(Expressions.statement(Expressions.assign(aggregateState, Expressions.new_(WindowOperator.AggregateState.class))));

    for (StreamingAggImpState aggImpState : aggs) {
      // Use reset to initialize aggregate state
      initAggState.add(
          Expressions.statement(
              Expressions.call(aggregateState,
                  SamzaBuiltInMethod.AGGSTATE_PUT.method,
                  Expressions.constant(aggImpState.aggIdx),
                  StreamingRexImplTable.getDefaultValue(javaClass(typeFactory, aggImpState.call.type)))));
    }

    initAggState.add(Expressions.statement(Expressions.call(partitions, SamzaBuiltInMethod.MAP_PUT.method, partitionKey, aggregateState)));

    parent.add(Expressions.ifThen(Expressions.equal(aggregateState, Expressions.constant(null)),
        initAggState.toBlock()));
  }

  private void generateUpdateAndEmit(final BlockBuilder parent, Window.Group group, Integer groupId,
                                     String pKeySuffix,
                                     List<StreamingAggImpState> aggs,
                                     final PhysType inputRowType, final PhysType outputType,
                                     final RexToLixTranslator.InputGetter inputGetter,
                                     final Expression inputRow,
                                     final ParameterExpression inputTuple,
                                     final List<Expression> translatedConstants,
                                     final List<RexLiteral> constants,
                                     final ParameterExpression collector) {
    final ParameterExpression partitionKey = Expressions.parameter(WindowOperator.PartitionKey.class,
        "partitionKey" + pKeySuffix);
    final ParameterExpression aggregateState = Expressions.parameter(WindowOperator.AggregateState.class,
        "aggregateState" + pKeySuffix);

    derivePartitionKey(parent, partitionKey, group, inputRowType, pKeySuffix, inputGetter);
    getAndInitAggregateStateIfNull(parent, aggregateState, partitionKey,
        Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
            SamzaBuiltInMethod.WINDOWOP_GET_PARTITIONS.method, Expressions.constant(groupId)),
        aggs);

    for (final StreamingAggImpState aggImpState : aggs) {
      final List<Expression> accumulator = new ArrayList<Expression>();
      Expression acc = Expressions.call(aggregateState, SamzaBuiltInMethod.AGGSTATE_GET.method, Expressions.constant(aggImpState.aggIdx));
      accumulator.add(acc);

      parent.add(Expressions.statement(
          Expressions.call(aggregateState, SamzaBuiltInMethod.AGGSTATE_PUT.method,
              Expressions.constant(aggImpState.aggIdx),
              aggImpState.implementor.implementStreamingAdd(
                  new StreamingAggContext() {
                    @Override
                    public SqlAggFunction aggregation() {
                      return aggImpState.call.getAggregation();
                    }

                    @Override
                    public RelDataType returnRelType() {
                      return aggImpState.call.getType();
                    }

                    @Override
                    public Type returnType() {
                      return javaClass(typeFactory, returnRelType());
                    }

                    @Override
                    public List<? extends RelDataType> parameterRelTypes() {
                      return fieldRowTypes(inputRowType.getRowType(), constants, aggImpState.call.getArgList());
                    }

                    @Override
                    public List<? extends Type> parameterTypes() {
                      return fieldTypes(typeFactory, parameterRelTypes());
                    }
                  },
                  new StreamingAggAddContext() {
                    @Override
                    public List<RexNode> rexArguments() {
                      List<RelDataTypeField> inputTypes =
                          inputRowType.getRowType().getFieldList();
                      List<RexNode> args = new ArrayList<RexNode>();
                      for (int index : aggImpState.call.getArgList()) {
                        args.add(RexInputRef.of(index, inputTypes));
                      }
                      return args;
                    }

                    @Override
                    public RexNode rexFilterArgument() {
                      return aggImpState.call.filterArg < 0
                          ? null
                          : RexInputRef.of(aggImpState.call.filterArg,
                          inputRowType.getRowType());
                    }

                    @Override
                    public List<Expression> arguments() {
                      return rowTranslator().translateList(rexArguments());
                    }

                    @Override
                    public RexToLixTranslator rowTranslator() {
                      return RexToLixTranslator.forAggregation(typeFactory,
                          currentBlock(), inputGetter).setNullable(currentNullables());
                    }

                    @Override
                    public List<Expression> accumulator() {
                      return accumulator;
                    }

                    @Override
                    public BlockBuilder nestBlock() {
                      return null;
                    }

                    @Override
                    public void nestBlock(BlockBuilder block) {
                    }

                    @Override
                    public void nestBlock(BlockBuilder block, Map<RexNode, Boolean> nullables) {
                    }

                    @Override
                    public BlockBuilder currentBlock() {
                      return parent;
                    }

                    @Override
                    public Map<RexNode, Boolean> currentNullables() {
                      return null;
                    }

                    @Override
                    public void exitBlock() {
                    }
                  }))));
    }

    // Populate output array
    ParameterExpression outputArray = Expressions.parameter(Object[].class, "result" + pKeySuffix);

    final List<Expression> outputRow = new ArrayList<Expression>();
    int fieldCountWithAggResults =
        inputRowType.getRowType().getFieldCount();
    for (int i = 0; i < fieldCountWithAggResults; i++) {
      outputRow.add(
          inputRowType.fieldReference(
              inputRow, i,
              outputType.getJavaFieldType(i)));
    }

    for (StreamingAggImpState aggImpState : aggs) {
      outputRow.add(Expressions.call(aggregateState, SamzaBuiltInMethod.AGGSTATE_GET.method, Expressions.constant(aggImpState.aggIdx)));
    }

    parent.add(Expressions.declare(0, outputArray, outputType.record(outputRow)));

    Expression creatingOutputTuple =
        Expressions.call(SamzaBuiltInMethod.INTERMEDIATE_TUPLE_FROM_TUPLE_AND_CONTENT.method, inputTuple, outputArray,
            Expressions.call(Expressions.parameter(WindowOperator.class, "this"), SamzaBuiltInMethod.WINDOWOP_GET_OUTPUT_STREAM_NAME.method));
//    Expression outgoingEnvelope = Expressions.new_(OutgoingMessageEnvelope.class,
//        Expressions.call(Expressions.parameter(WindowOperator.class, "this"), SamzaBuiltInMethod.WINDOWOP_GET_OUTPUT_STREAM.method),
//        creatingOutputTuple);
    ParameterExpression exception = Expressions.parameter(Exception.class, "e");
    parent.add(
        Expressions.tryCatch(
            Expressions.statement(Expressions.call(collector, SamzaBuiltInMethod.COLLECTOR_SEND.method, creatingOutputTuple)),
            Expressions.catch_(
                exception,
                Expressions.throw_(Expressions.new_(RuntimeException.class, exception)))));
  }

  private Expression generateAdjustAggregate(Window.Group group, Integer groupId, List<StreamingAggImpState> aggs, final PhysType inputRowType,
                                             final List<Expression> translatedConstants, final List<RexLiteral> constants) {

    final BlockBuilder adjustAggregateBody = new BlockBuilder();

    final ParameterExpression partitionKey = Expressions.parameter(WindowOperator.PartitionKey.class, "partitionKey");

    final ParameterExpression tupleParam = Expressions.parameter(0, IntermediateMessageTuple.class, adjustAggregateBody.newName("tuple"));
    ParameterExpression partitions = Expressions.parameter(0, Map.class, adjustAggregateBody.newName("partitions"));

    final WindowRelInputGetter inputGetter = new WindowRelInputGetter(
        Expressions.call(tupleParam, SamzaBuiltInMethod.INTERMEDIATE_TUPLE_GET_CONTENT.method), inputRowType,
        inputRowType.getRowType().getFieldCount(), translatedConstants);

    derivePartitionKey(adjustAggregateBody, partitionKey, group, inputRowType, String.valueOf(groupId), inputGetter);

    // Get the aggregate state
    ParameterExpression aggState = Expressions.parameter(WindowOperator.AggregateState.class, "aggState");
    getAndInitAggregateStateIfNull(adjustAggregateBody, aggState, partitionKey, partitions, aggs);

    // Iterate over aggregate calls and update each aggregate accordingly
    for (final StreamingAggImpState aggImpState : aggs) {
      final List<Expression> accumulator = new ArrayList<Expression>();
      Expression acc = Expressions.call(aggState, SamzaBuiltInMethod.AGGSTATE_GET.method, Expressions.constant(aggImpState.aggIdx));
      accumulator.add(acc);

      adjustAggregateBody.add(Expressions.statement(
          Expressions.call(aggState, SamzaBuiltInMethod.AGGSTATE_PUT.method, Expressions.constant(aggImpState.aggIdx),
              aggImpState.implementor.implementSubtract(
                  new StreamingAggContext() {
                    @Override
                    public SqlAggFunction aggregation() {
                      return aggImpState.call.getAggregation();
                    }

                    @Override
                    public RelDataType returnRelType() {
                      return aggImpState.call.getType();
                    }

                    @Override
                    public Type returnType() {
                      return javaClass(typeFactory, returnRelType());
                    }

                    @Override
                    public List<? extends RelDataType> parameterRelTypes() {
                      return fieldRowTypes(inputRowType.getRowType(), constants, aggImpState.call.getArgList());
                    }

                    @Override
                    public List<? extends Type> parameterTypes() {
                      return fieldTypes(typeFactory, parameterRelTypes());
                    }
                  },
                  new StreamingAggSubtractContext() {
                    @Override
                    public List<RexNode> rexArguments() {
                      List<RelDataTypeField> inputTypes =
                          inputRowType.getRowType().getFieldList();
                      List<RexNode> args = new ArrayList<RexNode>();
                      for (int index : aggImpState.call.getArgList()) {
                        args.add(RexInputRef.of(index, inputTypes));
                      }
                      return args;
                    }

                    @Override
                    public RexNode rexFilterArgument() {
                      return aggImpState.call.filterArg < 0
                          ? null
                          : RexInputRef.of(aggImpState.call.filterArg,
                          inputRowType.getRowType());
                    }

                    @Override
                    public List<Expression> arguments() {
                      return rowTranslator().translateList(rexArguments());
                    }

                    @Override
                    public RexToLixTranslator rowTranslator() {
                      return RexToLixTranslator.forAggregation(typeFactory,
                          currentBlock(), inputGetter).setNullable(currentNullables());
                    }

                    @Override
                    public List<Expression> accumulator() {
                      return accumulator;
                    }

                    @Override
                    public BlockBuilder nestBlock() {
                      return null;
                    }

                    @Override
                    public void nestBlock(BlockBuilder block) {
                    }

                    @Override
                    public void nestBlock(BlockBuilder block, Map<RexNode, Boolean> nullables) {
                    }

                    @Override
                    public BlockBuilder currentBlock() {
                      return adjustAggregateBody;
                    }

                    @Override
                    public Map<RexNode, Boolean> currentNullables() {
                      return null;
                    }

                    @Override
                    public void exitBlock() {
                    }
                  }))));
    }
    adjustAggregateBody.add(Expressions.return_(null, Expressions.constant(null)));

    ParameterExpression tupleParamObj = Expressions.parameter(0, Object.class, adjustAggregateBody.newName("tuple"));
    ParameterExpression partitionsObj = Expressions.parameter(0, Object.class, adjustAggregateBody.newName("partitions"));

    BlockBuilder callTypedMethod = new BlockBuilder();
    callTypedMethod.add(Expressions.call(Expressions.parameter(Function2.class, "this"), SamzaBuiltInMethod.FUNC2_APPLY.method,
        Expressions.convert_(tupleParamObj, IntermediateMessageTuple.class),
        Expressions.convert_(partitionsObj, Map.class)));

    final List<MemberDeclaration> memberDeclarations = Expressions.<MemberDeclaration>list(
        Expressions.methodDecl(Modifier.PUBLIC, Void.class, "apply", ImmutableList.of(tupleParam, partitions),
            adjustAggregateBody.toBlock()),
        Expressions.methodDecl(Modifier.PUBLIC, Void.class, "apply", ImmutableList.of(tupleParamObj, partitionsObj),
            callTypedMethod.toBlock()));

    return Expressions.new_(Function2.class,
        Collections.<Expression>emptyList(),
        memberDeclarations);
  }

  private RelDataType getFieldType(int fieldIndex, RelDataType rowType) {
    for (RelDataTypeField dataTypeField : rowType.getFieldList()) {
      if (dataTypeField.getIndex() == fieldIndex) {
        return dataTypeField.getType();
      }
    }

    throw new SamzaException(
        String.format("Invalid row type. Field with index %d is not present in given row type %s", fieldIndex, rowType));
  }

  private boolean isValidOrderByFieldType(RelFieldCollation relFieldCollation, RelDataType relDataType) {
    // order by field type for time based sliding windows is limited to TIMESTAMP
    // TODO: Add support for other types (DATE, TIME) as needed
    for (RelDataTypeField field : relDataType.getFieldList()) {
      if (field.getIndex() == relFieldCollation.getFieldIndex()) {
        SqlTypeName fieldType = field.getType().getSqlTypeName();
        return fieldType == SqlTypeName.TIMESTAMP &&
            (relFieldCollation.getDirection() == RelFieldCollation.Direction.ASCENDING ||
                relFieldCollation.getDirection() == RelFieldCollation.Direction.STRICTLY_ASCENDING);
      }
    }

    throw new SamzaException("Invalid field collation.");
  }

  private Expression translateLowerBoundUpdate(RexWindowBound lowerBound,
                                               RexToLixTranslator translator,
                                               List<RelFieldCollation> fieldCollations,
                                               PhysType physType) {
    // substract preceding interval from upper bound and convert it to milliseconds form epoch
    int orderKey =
        fieldCollations.get(0).getFieldIndex();
    RelDataType keyType =
        physType.getRowType().getFieldList().get(orderKey).getType();
    Type desiredKeyType = typeFactory.getJavaClass(keyType);
    if (lowerBound.getOffset() == null) {
      desiredKeyType = Primitive.box(desiredKeyType);
    }
    Expression val = translator.translate(
        new RexInputRef(orderKey, keyType), desiredKeyType);
    if (!lowerBound.isCurrentRow()) {
      RexNode node = lowerBound.getOffset();
      Expression offs = translator.translate(node);
      val = Expressions.subtract(val, offs);
    }
    return val;
  }

  private Statement generateInitWindowStore(int groupId) {
    /*
     * setWindowState(groupId)
     */
    return Expressions.statement(Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
        SamzaBuiltInMethod.WINDOWOP_INIT_WINDOW_STORE.method, Expressions.constant(groupId)));
  }

  private Statement generateInitMessageStore(int groupId) {
    return Expressions.statement(Expressions.call(Expressions.parameter(WindowOperator.class, "this"),
        SamzaBuiltInMethod.WINDOWOP_INIT_MESSAGE_STORE.method, Expressions.constant(groupId)));
  }

  private Statement generateInitGroupState(int groupId, Long precededBy) {
    return Expressions.statement(Expressions.call(
        Expressions.parameter(WindowOperator.class, "this"),
        SamzaBuiltInMethod.WINDOWOP_INIT_GROUP_STATE.method,
        Expressions.constant(groupId), Expressions.constant(precededBy)));
  }

  private WindowOperator genClass(BlockStatement realProcessBody, final ParameterExpression tuple, final ParameterExpression collector, final ParameterExpression coordinator) {
    String className = "GeneratedWindowOperator" + windowOperatorCounter.getAndIncrement();
    final ArrayList<MemberDeclaration> declarations = new ArrayList<MemberDeclaration>();

    // protected abstract void realProcess(Tuple ituple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception;
    declarations.add(Expressions.methodDecl(Modifier.PUBLIC, void.class, SamzaBuiltInMethod.WINDOWOP_REALPROCESS.method.getName(),
        ImmutableList.<ParameterExpression>of(tuple, collector, coordinator), realProcessBody));

    final ClassDeclaration classDeclaration =
        Expressions.classDecl(Modifier.PUBLIC, className, WindowOperator.class, Collections.<Type>emptyList(), declarations);

    try {
      return createInstance(classDeclaration, Expressions.toString(declarations, "\n", false));
    } catch (Exception e) {
      throw new SamzaException("Cannot generate WindowOperator implementation.", e);
    }
  }

  private WindowOperator createInstance(ClassDeclaration classDeclaration, String body) throws IOException, CompileException {
    ICompilerFactory compilerFactory;
    try {
      compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to instantiate java compiler", e);
    }
    IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
    cbe.setClassName(classDeclaration.name);
    cbe.setExtendedClass(WindowOperator.class);
    cbe.setImplementedInterfaces(classDeclaration.implemented.toArray(new Class[classDeclaration.implemented.size()]));
    cbe.setParentClassLoader(WindowOperatorGenerator.class.getClassLoader());
    cbe.setDebuggingInformation(true, true, true);

    System.out.println(body);
    return (WindowOperator) cbe.createInstance(new StringReader(body));
  }

  public static Type javaClass(
      JavaTypeFactory typeFactory, RelDataType type) {
    final Type clazz = typeFactory.getJavaClass(type);
    return clazz instanceof Class ? clazz : Object[].class;
  }

  public static List<RelDataType> fieldRowTypes(
      final RelDataType inputRowType,
      final List<? extends RexNode> extraInputs,
      final List<Integer> argList) {
    final List<RelDataTypeField> inputFields = inputRowType.getFieldList();
    return new AbstractList<RelDataType>() {
      public RelDataType get(int index) {
        final int arg = argList.get(index);
        return arg < inputFields.size()
            ? inputFields.get(arg).getType()
            : extraInputs.get(arg - inputFields.size()).getType();
      }

      public int size() {
        return argList.size();
      }
    };
  }

  public static List<Type> fieldTypes(
      final JavaTypeFactory typeFactory,
      final List<? extends RelDataType> inputTypes) {
    return new AbstractList<Type>() {
      public Type get(int index) {
        return javaClass(typeFactory, inputTypes.get(index));
      }

      public int size() {
        return inputTypes.size();
      }
    };
  }

  /**
   * Implementation of {@link RexToLixTranslator.InputGetter}
   * suitable for generating implementations of windowed aggregate
   * functions.
   * <p/>
   * Copied from Calcite on 10/12/2015.
   */
  private static class WindowRelInputGetter
      implements RexToLixTranslator.InputGetter {
    private final Expression row;
    private final PhysType rowPhysType;
    private final int actualInputFieldCount;
    private final List<Expression> constants;

    private WindowRelInputGetter(Expression row,
                                 PhysType rowPhysType, int actualInputFieldCount,
                                 List<Expression> constants) {
      this.row = row;
      this.rowPhysType = rowPhysType;
      this.actualInputFieldCount = actualInputFieldCount;
      this.constants = constants;
    }

    public Expression field(BlockBuilder list, int index, Type storageType) {
      if (index < actualInputFieldCount) {
        Expression current = list.append("current", row);
        return rowPhysType.fieldReference(current, index, storageType);
      }
      return constants.get(index - actualInputFieldCount);
    }
  }
}


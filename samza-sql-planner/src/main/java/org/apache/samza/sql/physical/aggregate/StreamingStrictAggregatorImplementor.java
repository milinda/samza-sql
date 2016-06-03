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

package org.apache.samza.sql.physical.aggregate;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.lang.reflect.Type;
import java.util.*;

/**
 * The base implementation of strict streaming aggregate function based on
 * {@link org.apache.calcite.adapter.enumerable.StrictAggImplementor}.
 * <p/>
 * TODO: Try to extend {@link org.apache.calcite.adapter.enumerable.StrictAggImplementor}, instead of copying the code.
 */
public abstract class StreamingStrictAggregatorImplementor implements StreamingAggImplementor {
  private boolean needTrackEmptySet;
  private boolean trackNullsPerRow = false;
  private int stateSize;

  protected boolean nonDefaultOnEmptySet(AggContext info) {
    return info.returnRelType().isNullable();
  }

  protected final void accAdvance(AggAddContext add, Expression acc,
                                  Expression next) {
    add.currentBlock().add(
        Expressions.statement(
            Expressions.assign(acc, Types.castIfNecessary(acc.type, next))));
  }

  @Override
  public List<Type> getStateType(AggContext info) {
    List<Type> subState = getNotNullState(info);
    stateSize = subState.size();
    needTrackEmptySet = nonDefaultOnEmptySet(info);
    if (!needTrackEmptySet) {
      return subState;
    }

    for (RelDataType type : info.parameterRelTypes()) {
      if (type.isNullable()) {
        trackNullsPerRow = true;
        break;
      }
    }

    List<Type> res = new ArrayList<Type>(subState.size() + 1);
    res.addAll(subState);
    res.add(boolean.class); // has not nulls
    return res;
  }

  public List<Type> getNotNullState(AggContext info) {
    return Collections.singletonList(Primitive.unbox(info.returnType()));
  }

  @Override
  public void implementReset(AggContext info, AggResetContext reset) {
    BlockBuilder currentBlock = reset.currentBlock();
    Expression accumulator = reset.accumulator().get(0);
    currentBlock.add(
        Expressions.statement(
            Expressions.assign(accumulator, StreamingRexImplTable.getDefaultValue(accumulator.getType()))));
  }

  @Override
  public void implementAdd(AggContext info, AggAddContext add) {
    final List<RexNode> args = add.rexArguments();
    final RexToLixTranslator translator = add.rowTranslator();
    final List<Expression> conditions = new ArrayList<Expression>();
    conditions.addAll(
        translator.translateList(args, RexImpTable.NullAs.IS_NOT_NULL));
    if (add.rexFilterArgument() != null) {
      conditions.add(
          translator.translate(add.rexFilterArgument(),
              RexImpTable.NullAs.FALSE));
    }
    Expression condition = Expressions.foldAnd(conditions);
    if (Expressions.constant(false).equals(condition)) {
      return;
    }

    boolean argsNotNull = Expressions.constant(true).equals(condition);
    final BlockBuilder thenBlock =
        argsNotNull
            ? add.currentBlock()
            : new BlockBuilder(true, add.currentBlock());
    if (trackNullsPerRow) {
      // TODO: We don't need to track nulls per row?
      List<Expression> acc = add.accumulator();
      thenBlock.add(
          Expressions.statement(
              Expressions.assign(acc.get(acc.size() - 1),
                  Expressions.constant(true))));
    }
    if (argsNotNull) {
      implementNotNullAdd(info, add);
      return;
    }

    final Map<RexNode, Boolean> nullables = new HashMap<RexNode, Boolean>();
    for (RexNode arg : args) {
      if (translator.isNullable(arg)) {
        nullables.put(arg, false);
      }
    }
    add.nestBlock(thenBlock, nullables);
    implementNotNullAdd(info, add);
    add.exitBlock();
    add.currentBlock().add(Expressions.ifThen(condition, thenBlock.toBlock()));
  }

  protected abstract void implementNotNullAdd(AggContext info, AggAddContext add);


  @Override
  public Expression implementSubtract(StreamingAggContext info, StreamingAggSubtractContext sub) {
    final List<RexNode> args = sub.rexArguments();
    final RexToLixTranslator translator = sub.rowTranslator();
    final List<Expression> conditions = new ArrayList<Expression>();
    conditions.addAll(
        translator.translateList(args, RexImpTable.NullAs.IS_NOT_NULL));
    if (sub.rexFilterArgument() != null) {
      conditions.add(
          translator.translate(sub.rexFilterArgument(),
              RexImpTable.NullAs.FALSE));
    }
    Expression condition = Expressions.foldAnd(conditions);
    if (Expressions.constant(false).equals(condition)) {
      return sub.accumulator().get(0);
    }

    boolean argsNotNull = Expressions.constant(true).equals(condition);

    if (argsNotNull) {
      return implementNotNullSubtract(info, sub);
    }

    return Expressions.makeTernary(ExpressionType.Conditional, condition, implementNotNullSubtract(info, sub),
        sub.accumulator().get(0));
  }

  protected abstract Expression implementNotNullSubtract(StreamingAggContext info, StreamingAggSubtractContext sub);

  @Override
  public Expression implementResult(AggContext info, AggResultContext result) {
    return null;
  }
}

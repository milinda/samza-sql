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

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import org.apache.calcite.adapter.enumerable.AggAddContext;
import org.apache.calcite.adapter.enumerable.AggContext;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.sql.SqlAggFunction;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Map;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.*;

public class StreamingRexImplTable {

  private final Map<SqlAggFunction, Supplier<? extends StreamingAggImplementor>>
      winAggMap = Maps.newHashMap();

  StreamingRexImplTable() {
    winAggMap.put(COUNT, constructorSupplier(StreamingCountImplementor.class));
    winAggMap.put(SUM, constructorSupplier(StreamingSumImplementor.class));
    winAggMap.put(SUM0, constructorSupplier(StreamingSumImplementor.class));
  }

  public static final StreamingRexImplTable INSTANCE = new StreamingRexImplTable();

  public StreamingAggImplementor get(final SqlAggFunction sqlAggFunction) {
    Supplier<? extends StreamingAggImplementor> streamingAgg = winAggMap.get(sqlAggFunction);
    if (streamingAgg != null) {
      return streamingAgg.get();
    }

    // TODO: Throw error?
    return null;
  }

  public static class StreamingCountImplementor extends StreamingStrictAggregatorImplementor implements StreamingAggImplementor {

    @Override
    protected Expression implementNotNullSubtract(StreamingAggContext info, StreamingAggSubtractContext sub) {
      Expression acc = sub.accumulator().get(0);
      return Expressions.makeTernary(ExpressionType.Conditional,
          Expressions.equal(acc, Expressions.constant(null)),
          Types.castIfNecessary(info.returnType(), acc),
          Expressions.subtract(Types.castIfNecessary(info.returnType(), acc),
              Expressions.constant(1)));
    }

    @Override
    protected void implementNotNullAdd(AggContext info, AggAddContext add) {
      add.currentBlock().add(
          Expressions.statement(
              Expressions.postIncrementAssign(add.accumulator().get(0))));
    }

    @Override
    public Expression implementStreamingAdd(StreamingAggContext info, StreamingAggAddContext add) {
      Expression acc = add.accumulator().get(0);
      return Expressions.makeTernary(ExpressionType.Conditional,
          Expressions.equal(acc, Expressions.constant(null)),
          Types.castIfNecessary(info.returnType(), acc),
          Expressions.add(Types.castIfNecessary(info.returnType(), acc),
              Expressions.constant(1)));
    }
  }

  public static class StreamingSumImplementor extends StreamingStrictAggregatorImplementor implements StreamingAggImplementor {

    @Override
    protected void implementNotNullAdd(AggContext info, AggAddContext add) {
      Expression acc = add.accumulator().get(0);
      Expression next;
      if (info.returnType() == BigDecimal.class) {
        next = Expressions.call(acc, "add", add.arguments().get(0));
      } else {
        next = Expressions.add(acc,
            Types.castIfNecessary(acc.type, add.arguments().get(0)));
      }
      accAdvance(add, acc, next);
    }

    @Override
    protected Expression implementNotNullSubtract(StreamingAggContext info, StreamingAggSubtractContext sub) {
      Expression acc = sub.accumulator().get(0);
      Expression next;
      if (info.returnType() == BigDecimal.class) {
        next = Expressions.makeTernary(ExpressionType.Conditional,
            Expressions.equal(acc, Expressions.constant(null)),
            acc,
            Expressions.call(acc, "subtract", sub.arguments().get(0)));
      } else {
        next = Expressions.makeTernary(ExpressionType.Conditional,
            Expressions.equal(acc, Expressions.constant(null)),
            Types.castIfNecessary(info.returnType(), acc),
            Expressions.subtract(Types.castIfNecessary(info.returnType(), acc),
                Types.castIfNecessary(info.returnType(), sub.arguments().get(0))));
      }

      return next;
    }

    @Override
    public Expression implementStreamingAdd(StreamingAggContext info, StreamingAggAddContext add) {
      Expression acc = add.accumulator().get(0);
      Expression next;
      if (info.returnType() == BigDecimal.class) {
        next = Expressions.makeTernary(ExpressionType.Conditional,
            Expressions.equal(acc, Expressions.constant(null)),
            acc,
            Expressions.call(acc, "add", acc));
      } else {
        next = Expressions.makeTernary(ExpressionType.Conditional,
            Expressions.equal(acc, Expressions.constant(null)),
            Types.castIfNecessary(info.returnType(), acc),
            Expressions.add(Types.castIfNecessary(info.returnType(), acc),
                Types.castIfNecessary(info.returnType(), add.arguments().get(0))));
      }

      return next;
    }
  }

  private <T> Supplier<T> constructorSupplier(Class<T> klass) {
    final Constructor<T> constructor;
    try {
      constructor = klass.getDeclaredConstructor();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          klass + " should implement zero arguments constructor");
    }
    return new Supplier<T>() {
      public T get() {
        try {
          return constructor.newInstance();
        } catch (InstantiationException e) {
          throw new IllegalStateException(
              "Unable to instantiate aggregate implementor " + constructor, e);
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(
              "Error while creating aggregate implementor " + constructor, e);
        } catch (InvocationTargetException e) {
          throw new IllegalStateException(
              "Error while creating aggregate implementor " + constructor, e);
        }
      }
    };
  }

  public static Expression getDefaultValue(Type type) {
    if (Primitive.is(type)) {
      Primitive p = Primitive.of(type);
      return Expressions.constant(p.defaultValue, type);
    }
    return Expressions.constant(null, type);
  }
}

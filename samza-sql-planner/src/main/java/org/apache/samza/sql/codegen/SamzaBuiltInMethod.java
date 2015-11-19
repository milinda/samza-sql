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

package org.apache.samza.sql.codegen;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ObjectArrays;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.physical.window.codegen.WindowOperator;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

public enum SamzaBuiltInMethod {
  WINDOWOP_REALPROCESS(org.apache.samza.sql.physical.window.codegen.WindowOperator.class, "realProcess", Tuple.class, SimpleMessageCollector.class, TaskCoordinator.class),
  WINDOWOP_GET_GROUP_STATE(org.apache.samza.sql.physical.window.codegen.WindowOperator.class, "getGroupState", Integer.class),
  WINDOWOP_INIT_WINDOW_STORE(org.apache.samza.sql.physical.window.codegen.WindowOperator.class, "initWindowStore", Integer.class),
  WINDOWOP_INIT_MESSAGE_STORE(org.apache.samza.sql.physical.window.codegen.WindowOperator.class, "initMessageStore", Integer.class),
  WINDOWOP_INIT_GROUP_STATE(org.apache.samza.sql.physical.window.codegen.WindowOperator.class, "initGroupState", Integer.class, Long.class),
  WINDOWOP_GET_WINDOW_STORE(org.apache.samza.sql.physical.window.codegen.WindowOperator.class, "getWindowStore", Integer.class),
  WINDOWOP_GET_MESSAGE_STORE(org.apache.samza.sql.physical.window.codegen.WindowOperator.class, "getMessageStore", Integer.class),
  WINDOWOP_IS_REPLAY(WindowOperator.class, "isReplay", Integer.class, Long.class, IntermediateMessageTuple.class),
  WINDOWOP_ADD_TO_MESSAGE_STORE(WindowOperator.class, "addToMessageStore", Integer.class, OrderedStoreKey.class, Tuple.class),
  WINDOWOP_UPDATE_LOWER_BOUND(WindowOperator.class, "updateLowerBound", Integer.class, Long.class),
  WINDOWOP_UPDATE_UPPER_BOUND(WindowOperator.class, "updateUpperBound", Integer.class, Long.class),
  WINDOWOP_GET_UPPER_BOUND(WindowOperator.class, "getUpperBound", Integer.class),
  WINDOWOP_GET_LOWER_BOUND(WindowOperator.class, "getLowerBound", Integer.class),
  WINDOWOP_ADD_MESSAGE(WindowOperator.class, "addMessage", Integer.class, Long.class, IntermediateMessageTuple.class),
  WINDOWOP_GET_AGGREATE_STATE(WindowOperator.class, "getAggregateState", Integer.class, WindowOperator.PartitionKey.class),
  WINDOWOP_PUT_AGGREATE_STATE(WindowOperator.class, "putAggregateState", Integer.class, WindowOperator.PartitionKey.class, WindowOperator.AggregateState.class),
  WINDOWOP_PURGE_MESSAGES(WindowOperator.class, "purgeMessages", Integer.class, Function2.class),
  WINDOWOP_INIT_PARTITIONS(WindowOperator.class, "initPartitions", Integer.class),
  WINDOWOP_GET_PARTITIONS(WindowOperator.class, "getPartitions", Integer.class),
  WINDOWOP_GET_OUTPUT_STREAM_NAME(WindowOperator.class, "getOutputStreamName"),
  INTERMEDIATE_TUPLE_GET_CONTENT(IntermediateMessageTuple.class, "getContent"),
  INTERMEDIATE_TUPLE_GET_OFFSET(IntermediateMessageTuple.class, "getOffset"),
  EXPR_EXECUTE1(org.apache.samza.sql.expr.Expression.class, "execute", Object[].class),
  EXPR_EXECUTE2(org.apache.samza.sql.expr.Expression.class, "execute", Object[].class, Object[].class),
  PKEY_BUILDER_SET(WindowOperator.PartitionKeyBuilder.class, "set", Integer.class, Object.class),
  PKEY_BUILDER_BUILD(WindowOperator.PartitionKeyBuilder.class, "build"),
  FUNC2_APPLY(Function2.class, "apply", Object.class, Object.class),
  MAP_GET(Map.class, "get", Object.class),
  MAP_PUT(Map.class, "put", Object.class, WindowOperator.AggregateState.class),
  AGGSTATE_PUT(WindowOperator.AggregateState.class, "put", Integer.class, Object.class),
  AGGSTATE_GET(WindowOperator.AggregateState.class, "get", Integer.class),
  INTERMEDIATE_TUPLE_FROM_TUPLE_AND_CONTENT(IntermediateMessageTuple.class, "fromTupleAndContent",
      IntermediateMessageTuple.class, Object[].class, EntityName.class),
  COLLECTOR_SEND(SimpleMessageCollector.class, "send", Tuple.class);


  public final Method method;
  public final Constructor constructor;
  public final Field field;

  public static final ImmutableMap<Method, BuiltInMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, BuiltInMethod> builder =
        ImmutableMap.builder();
    for (BuiltInMethod value : BuiltInMethod.values()) {
      if (value.method != null) {
        builder.put(value.method, value);
      }
    }
    MAP = builder.build();
  }

  private SamzaBuiltInMethod(Method method, Constructor constructor, Field field) {
    this.method = method;
    this.constructor = constructor;
    this.field = field;
  }

  /**
   * Defines a method.
   */
  SamzaBuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
    this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
  }

  /**
   * Defines a constructor.
   */
  SamzaBuiltInMethod(Class clazz, Class... argumentTypes) {
    this(null, Types.lookupConstructor(clazz, argumentTypes), null);
  }

  /**
   * Defines a field.
   */
  SamzaBuiltInMethod(Class clazz, String fieldName, boolean dummy) {
    this(null, null, Types.lookupField(clazz, fieldName));
    assert dummy : "dummy value for method overloading must be true";
  }
}
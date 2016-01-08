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

package org.apache.samza.sql.bench.join;

import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.expr.Expression;
import org.apache.samza.sql.operators.SimpleOperatorImpl;
import org.apache.samza.sql.operators.join.JoinSpec;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

public class JoinOperator extends SimpleOperatorImpl {

  private KeyValueStore<String, IntermediateMessageTuple> relationStore;

  private EntityName relationName;
  private boolean isLeftRelation;
  private boolean isRightRelaiton;
  private RelDataType type;
  private final JoinSpec spec;

  public JoinOperator(JoinSpec spec) {
    super(spec);
    this.spec = spec;
  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Tuple tuple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if (!(tuple instanceof IntermediateMessageTuple)) {
      throw new SamzaException("Unsupported insput tuple type: " + tuple.getClass());
    }

    final Function1 relationKeyDerivator = new org.apache.calcite.linq4j.function.Function1() {
      public String apply(Object[] v1) {
        return v1[0] == null ? (String) null : v1[0].toString();
      }

      public Object apply(Object v1) {
        return apply(
            (Object[]) v1);
      }
    };

    final Function1 streamKeyDerivator = new org.apache.calcite.linq4j.function.Function1() {
      public String apply(Object[] v1) {
        return v1[4] == null ? (String) null : v1[4].toString();
      }

      public Object apply(Object v1) {
        return apply(
            (Object[]) v1);
      }
    };

    final Function2 joinSelector = new org.apache.calcite.linq4j.function.Function2() {
      public Object[] apply(Object[] left, Object[] right) {
        return new Object[]{
            left[0],
            left[1],
            left[2],
            left[3],
            left[4],
            right[0],
            right[1]};
      }

      public Object[] apply(Object left, Object right) {
        return apply(
            (Object[]) left,
            (Object[]) right);
      }
    };

    IntermediateMessageTuple iTuple = (IntermediateMessageTuple) tuple;
    if (iTuple.getEntityName().equals(relationName)) {
      // Loading relation to local storage
      relationStore.put((String)relationKeyDerivator.apply(iTuple.getContent()), iTuple);
    } else {
      // Join operation
      IntermediateMessageTuple matched = relationStore.get((String) streamKeyDerivator.apply(iTuple.getContent()));
      if(matched != null) {
        collector.send(IntermediateMessageTuple.fromData(
            (Object[]) joinSelector.apply(iTuple.getContent(), matched.getContent()),
            iTuple.getKey(), iTuple.getCreationTimeMillis(), iTuple.getOffset(), false, spec.getOutputName()));
      }
    }
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    this.relationStore = (KeyValueStore<String, IntermediateMessageTuple>) context.getStore("products");
  }
}

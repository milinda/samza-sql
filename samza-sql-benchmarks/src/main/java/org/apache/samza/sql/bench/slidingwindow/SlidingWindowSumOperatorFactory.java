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

package org.apache.samza.sql.bench.slidingwindow;

import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.samza.sql.physical.window.WindowOperatorSpec;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.HashMap;

public class SlidingWindowSumOperatorFactory {


  public static SlidingWindowOperator create(WindowOperatorSpec spec) {
    int groupCount = 1;

    Function1<Object[], Long> timestampSelector = new Function1<Object[], Long>() {
      @Override
      public Long apply(Object[] a0) {
        return SqlFunctions.toLong(a0[2]);
      }
    };

    Function0<Long> windowWiszeSelector = new Function0<Long>() {
      @Override
      public Long apply() {
        return 5 * 60000L;
      }
    };

    Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Void> messagePurger =
        new Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Void>() {
          @Override
          public Void apply(Object[] v0, KeyValueStore<PartitionKey, Object[]> v1) {
            PartitionKey.Builder partitionKeyBuilder0 = new PartitionKey.Builder(1);
            partitionKeyBuilder0.set(0, v0[0] == null ? (String) null : v0[0].toString());
            PartitionKey partitionKey = partitionKeyBuilder0.build();
            Object[] aggregateState = v1.get(partitionKey);
            if (aggregateState == null) {
              aggregateState = new Object[2];
              aggregateState[0] = 0L;
              aggregateState[1] = 0;
            } else {
              aggregateState[0] = ((Long) aggregateState[0]).longValue() - 1;
              aggregateState[1] = ((Integer) aggregateState[1]).intValue() - SqlFunctions.toInt(v0[1]);
            }

            v1.put(partitionKey, aggregateState);

            return null;
          }
        };

    Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Object[]> updateAggregateAndReturnResult =
        new Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Object[]>() {
          @Override
          public Object[] apply(Object[] v0, KeyValueStore<PartitionKey, Object[]> v1) {
            PartitionKey.Builder partitionKeyBuilder0 = new PartitionKey.Builder(1);
            partitionKeyBuilder0.set(0, v0[0] == null ? (String) null : v0[0].toString());
            PartitionKey partitionKey = partitionKeyBuilder0.build();
            Object[] aggregateState = v1.get(partitionKey);

            if (aggregateState == null) {
              aggregateState = new Object[2];
              aggregateState[0] = 0L;
              aggregateState[1] = 0;
            }

            aggregateState[0] = ((Long) aggregateState[0]).longValue() + 1;
            aggregateState[1] = ((Integer) aggregateState[1]).intValue() + SqlFunctions.toInt(v0[1]);

            v1.put(partitionKey, aggregateState);

            Object[] result = new Object[]{
                v0[0],
                v0[1],
                v0[2],
                aggregateState[0],
                aggregateState[1]};
            
            return result;
          }
        };

    HashMap<Integer, Function0<Long>> windowSizeSelectors = new HashMap<Integer, Function0<Long>>();
    windowSizeSelectors.put(0, windowWiszeSelector);

    HashMap<Integer, Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Void>> messagePurgers =
        new HashMap<Integer, Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Void>>();
    messagePurgers.put(0, messagePurger);

    HashMap<Integer, Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Object[]>> updaters =
        new HashMap<Integer, Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Object[]>>();
    updaters.put(0, updateAggregateAndReturnResult);


    return new SlidingWindowOperator(spec, groupCount, timestampSelector, windowSizeSelectors,
        messagePurgers, updaters);
  }
}

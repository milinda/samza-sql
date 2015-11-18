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

public class GeneratedWindowOp extends WindowOperator {
  public void realProcess(org.apache.samza.sql.api.data.Tuple tuple, org.apache.samza.task.sql.SimpleMessageCollector collector, org.apache.samza.task.TaskCoordinator coordinator) {
    org.apache.samza.sql.data.IntermediateMessageTuple intTuple = (org.apache.samza.sql.data.IntermediateMessageTuple) tuple;
    if (this.getWindowStore(0) == null) {
      this.initWindowStore(0);
    }
    if (this.getMessageStore(0) == null) {
      this.initMessageStore(0);
    }
    if (this.getGroupState(0) == null) {
      this.initGroupState(0, 0L);
    }
    Long tupleTimestamp;
    final Object[] current = intTuple.getContent();
    tupleTimestamp = org.apache.calcite.runtime.SqlFunctions.toLong(current[2]);
    if (this.isReplay(0, tupleTimestamp, intTuple)) {
      return;
    }
    if (tupleTimestamp > this.getUpperBound(0)) {
      this.updateUpperBound(0, tupleTimestamp);
    }
    if (this.getLowerBound(0) == 9223372036854775807L) {
      this.updateLowerBound(0, tupleTimestamp);
    } else {
      Long newLowerBound = org.apache.calcite.runtime.SqlFunctions.toLong(current[2]) - 3600000L;
      if (newLowerBound > 0) {
        this.updateLowerBound(0, newLowerBound);
      }
    }
    this.addMessage(0, tupleTimestamp, intTuple);
    if (this.getPartitions(0) == null) {
      this.initPartitions(0);
    }
    org.apache.calcite.linq4j.function.Function2 aggregateAdjuster = new org.apache.calcite.linq4j.function.Function2() {
      public Void apply(org.apache.samza.sql.data.IntermediateMessageTuple tuple, java.util.Map partitions) {
        org.apache.samza.sql.physical.window.codegen.WindowOperator.PartitionKeyBuilder partitionKeyBuilder0 = new org.apache.samza.sql.physical.window.codegen.WindowOperator.PartitionKeyBuilder(
            64);
        final Object[] current = tuple.getContent();
        partitionKeyBuilder0.set(0, current[0] == null ? (String) null : current[0].toString());
        org.apache.samza.sql.physical.window.codegen.WindowOperator.PartitionKey partitionKey = partitionKeyBuilder0.build();
        org.apache.samza.sql.physical.window.codegen.WindowOperator.AggregateState aggState = (org.apache.samza.sql.physical.window.codegen.WindowOperator.AggregateState) partitions.get(partitionKey);
        if (aggState == null) {
          aggState = new org.apache.samza.sql.physical.window.codegen.WindowOperator.AggregateState();
          aggState.put(0, 0L);
          aggState.put(1, 0);
          partitions.put(partitionKey, aggState);
        }
        aggState.put(0, aggState.get(0) == null ? ((Long) aggState.get(0)).longValue() : ((Long) aggState.get(0)).longValue() - 1);
        aggState.put(1, aggState.get(1) == null ? ((Integer) aggState.get(1)).intValue() : ((Integer) aggState.get(1)).intValue() - org.apache.calcite.runtime.SqlFunctions.toInt(current[3]));
        return null;
      }

      public Void apply(Object tuple, Object partitions) {
        return this.apply((org.apache.samza.sql.data.IntermediateMessageTuple) tuple, (java.util.Map) partitions);
      }

    };
    this.purgeMessages(0, aggregateAdjuster);
    org.apache.samza.sql.physical.window.codegen.WindowOperator.PartitionKeyBuilder partitionKeyBuilder0 = new org.apache.samza.sql.physical.window.codegen.WindowOperator.PartitionKeyBuilder(
        64);
    partitionKeyBuilder0.set(0, current[0] == null ? (String) null : current[0].toString());
    org.apache.samza.sql.physical.window.codegen.WindowOperator.PartitionKey partitionKey0 = partitionKeyBuilder0.build();
    org.apache.samza.sql.physical.window.codegen.WindowOperator.AggregateState aggregateState0 = (org.apache.samza.sql.physical.window.codegen.WindowOperator.AggregateState) this.getPartitions(0).get(partitionKey0);
    if (aggregateState0 == null) {
      aggregateState0 = new org.apache.samza.sql.physical.window.codegen.WindowOperator.AggregateState();
      aggregateState0.put(0, 0L);
      aggregateState0.put(1, 0);
      this.getPartitions(0).put(partitionKey0, aggregateState0);
    }
    aggregateState0.put(0, aggregateState0.get(0) == null ? ((Long) aggregateState0.get(0)).longValue() : ((Long) aggregateState0.get(0)).longValue() + 1);
    aggregateState0.put(1, aggregateState0.get(1) == null ? ((Integer) aggregateState0.get(1)).intValue() : ((Integer) aggregateState0.get(1)).intValue() + org.apache.calcite.runtime.SqlFunctions.toInt(current[3]));
    Object[] result0 = new Object[]{
        (intTuple.getContent())[0],
        (intTuple.getContent())[1],
        (intTuple.getContent())[2],
        (intTuple.getContent())[3],
        aggregateState0.get(0),
        aggregateState0.get(1)};
    try {
      collector.send(org.apache.samza.sql.data.IntermediateMessageTuple.fromTupleAndContent(intTuple, result0, this.getOutputStreamName()));
    } catch (Exception e) {
      throw new RuntimeException(
          e);
    }
    this.addToMessageStore(0, new org.apache.samza.sql.window.storage.TimeAndOffsetKey(
        tupleTimestamp,
        intTuple.getOffset()), intTuple);
  }

}

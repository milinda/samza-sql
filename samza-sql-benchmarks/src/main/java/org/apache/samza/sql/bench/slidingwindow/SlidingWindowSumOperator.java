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

import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.operators.SimpleOperatorImpl;
import org.apache.samza.sql.physical.window.TimeBasedSlidingWindowAggregatorState;
import org.apache.samza.sql.physical.window.WindowOperatorSpec;
import org.apache.samza.sql.physical.window.WindowStore;
import org.apache.samza.sql.window.storage.MessageStore;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.sql.window.storage.TimeAndOffsetKey;
import org.apache.samza.sql.window.storage.TimeKey;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SlidingWindowSumOperator extends SimpleOperatorImpl {
  /**
   * Operator id
   */
  protected String id;

  /**
   * Store all the messages pass through this operator. Message retention period depends underlying local storage's
   * retention policy.
   */
  protected Map<Integer, MessageStore> messageStoreMap = new HashMap<Integer, MessageStore>();

  /**
   * A reference to {@link org.apache.samza.task.TaskContext} for creating window stores.
   */
  protected TaskContext taskContext;

  /**
   * Keep the current window contents for each group of window aggregates.
   */
  protected Map<Integer, WindowStore> windowStoreMap = new HashMap<Integer, WindowStore>();

  /**
   * Keep bounds of each group of window aggregates.
   */
  protected KeyValueStore<String, Long> groupStateStore;

  /**
   * Aggregation results
   */
  protected Map<Integer, KeyValueStore<PartitionKey, AggregateState>> aggregateStateStoreMap = new HashMap<Integer, KeyValueStore<PartitionKey, AggregateState>>();

  protected WindowOperatorSpec spec;

  public SlidingWindowSumOperator(WindowOperatorSpec spec) {
    super(spec);
    this.spec = spec;
    this.id = spec.getId();
  }

  public void setSpec(WindowOperatorSpec spec) {
    this.spec = spec;
  }

  @Override
  public WindowOperatorSpec getSpec() {
    return spec;
  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // Not in use yet. But will be important for implementing low water marks or other out of order handling logic
  }

  @Override
  protected void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    throw new UnsupportedOperationException("Relation processing is not supported by this operator.");
  }

  @Override
  public void realProcess(Tuple tuple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    IntermediateMessageTuple intTuple = (IntermediateMessageTuple) tuple;
    if (this.getWindowStore(0) == null) {
      this.initWindowStore(0);
    }
    if (this.getMessageStore(0) == null) {
      this.initMessageStore(0);
    }

    if (this.getAggregateStateStore(0) == null) {
      this.initAggregateStateStore(0);
    }

    Long tupleTimestamp;
    final Object[] current = intTuple.getContent();
    tupleTimestamp = SqlFunctions.toLong(current[2]);
    if (this.isReplay(0, tupleTimestamp, intTuple)) {
      return;
    }
    if (tupleTimestamp > this.getUpperBound(0)) {
      this.updateUpperBound(0, tupleTimestamp);
    }
    if (this.getLowerBound(0) == 9223372036854775807L) {
      this.updateLowerBound(0, tupleTimestamp);
    } else {
      Long newLowerBound = SqlFunctions.toLong(current[2]) - 3600000L;
      if (newLowerBound > 0) {
        this.updateLowerBound(0, newLowerBound);
      }
    }
    this.addMessage(0, tupleTimestamp, intTuple);
//    if (this.getPartitions(0) == null) {
//      this.initPartitions(0);
//    }
    Function2 aggregateAdjuster = new Function2() {
      public Void apply(IntermediateMessageTuple tuple, KeyValueStore partitions) {
        PartitionKeyBuilder partitionKeyBuilder0 = new PartitionKeyBuilder(1);
        final Object[] current = tuple.getContent();
        partitionKeyBuilder0.set(0, current[0] == null ? (String) null : current[0].toString());
        PartitionKey partitionKey = partitionKeyBuilder0.build();
        AggregateState aggState = (AggregateState) partitions.get(partitionKey);
        if (aggState == null) {
          aggState = new AggregateState();
          aggState.put(0, 0L);
          aggState.put(1, 0);
        }

        aggState.put(0, aggState.get(0) == null ? ((Long) aggState.get(0)).longValue() : ((Long) aggState.get(0)).longValue() - 1);
        aggState.put(1, aggState.get(1) == null ? ((Integer) aggState.get(1)).intValue() : ((Integer) aggState.get(1)).intValue() - SqlFunctions.toInt(current[1]));
        partitions.put(partitionKey, aggState);
        return null;
      }

      public Void apply(Object tuple, Object partitions) {
        return this.apply((IntermediateMessageTuple) tuple, (KeyValueStore) partitions);
      }

    };
    this.purgeMessages(0, aggregateAdjuster);
    PartitionKeyBuilder partitionKeyBuilder0 = new PartitionKeyBuilder(
        1);
    partitionKeyBuilder0.set(0, current[0] == null ? (String) null : current[0].toString());
    PartitionKey partitionKey0 = partitionKeyBuilder0.build();
    AggregateState aggregateState0 = (AggregateState) this.getPartitions(0).get(partitionKey0);
    if (aggregateState0 == null) {
      aggregateState0 = new AggregateState();
      aggregateState0.put(0, 0L);
      aggregateState0.put(1, 0);
    }
    aggregateState0.put(0, aggregateState0.get(0) == null ? ((Long) aggregateState0.get(0)).longValue() : ((Long) aggregateState0.get(0)).longValue() + 1);
    aggregateState0.put(1, aggregateState0.get(1) == null ? ((Integer) aggregateState0.get(1)).intValue() : ((Integer) aggregateState0.get(1)).intValue() + org.apache.calcite.runtime.SqlFunctions.toInt(current[1]));
    this.getPartitions(0).put(partitionKey0, aggregateState0);
    Object[] result0 = new Object[]{
        (intTuple.getContent())[0],
        (intTuple.getContent())[1],
        (intTuple.getContent())[2],
        aggregateState0.get(0),
        aggregateState0.get(1)};
    try {
      collector.send(IntermediateMessageTuple.fromTupleAndContent(intTuple, result0, this.getOutputStreamName()));
    } catch (Exception e) {
      throw new RuntimeException(
          e);
    }
    this.addToMessageStore(0, new TimeAndOffsetKey(
        tupleTimestamp,
        intTuple.getOffset()), intTuple);
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    taskContext = context;
    groupStateStore = (KeyValueStore<String, Long>) taskContext.getStore("group-state");
  }

  public void setId(String id) {
    this.id = id;
  }

  public WindowStore getWindowStore(Integer groupId) {
    return windowStoreMap.get(groupId);
  }

  public MessageStore getMessageStore(Integer groupId) {
    return messageStoreMap.get(groupId);
  }

  public KeyValueStore<PartitionKey, AggregateState> getAggregateStateStore(Integer groupId) {
    return aggregateStateStoreMap.get(groupId);
  }

  public void initWindowStore(Integer groupId) {
    this.windowStoreMap.put(groupId,
        new WindowStore((KeyValueStore<OrderedStoreKey, TimeBasedSlidingWindowAggregatorState>) taskContext.getStore(String.format("wnd-store-group-%d", groupId))));
  }

  public void initMessageStore(Integer groupId) {
    this.messageStoreMap.put(groupId,
        new MessageStore(
            new StoreStream((KeyValueStore<OrderedStoreKey, Tuple>) taskContext.getStore(String.format("msg-store-group-%d", groupId))),
            EntityName.getStreamName("kafka:msgstorelog"),
            null));
  }

  public void initAggregateStateStore(Integer groupId) {
    this.aggregateStateStoreMap.put(groupId,
        (KeyValueStore<PartitionKey, AggregateState>) taskContext.getStore(String.format("aggstatestore-group-%s", groupId)));
  }

  public boolean isReplay(Integer groupId, Long tupleTimestamp, IntermediateMessageTuple tuple) {
    return messageStoreMap.get(new TimeAndOffsetKey(tupleTimestamp, tuple.getOffset())) != null;
  }

  public void addToMessageStore(Integer groupId, OrderedStoreKey key, Tuple t) {
    messageStoreMap.get(groupId).put(key, t);
  }

  public EntityName getOutputStreamName() {
    return spec.getOutputName();
  }

  public void updateLowerBound(Integer groupId, Long timestamp) {
    groupStateStore.put("lower-" + groupId, timestamp);
  }

  public void updateUpperBound(Integer groupId, Long timestamp) {
    groupStateStore.put("upper-" + groupId, timestamp);
  }

  public Long getUpperBound(Integer groupId) {
    Long upper = groupStateStore.get("upper-" + groupId);
    return upper == null ? -1 : upper;
  }

  public Long getLowerBound(Integer groupId) {
    Long lower = groupStateStore.get("lower-" + groupId);
    return lower == null ? 9223372036854775807L : lower;
  }


  public void addMessage(Integer groupId, Long tupleTimestamp, IntermediateMessageTuple tuple) {
    OrderedStoreKey key = new TimeKey(tupleTimestamp);
    WindowStore windowStore = windowStoreMap.get(groupId);
    TimeBasedSlidingWindowAggregatorState aggregatorState = windowStore.get(key);

    if (aggregatorState != null) {
      aggregatorState.addTuple(tupleTimestamp, tuple.getOffset());
    } else {
      aggregatorState = new TimeBasedSlidingWindowAggregatorState(tupleTimestamp, tuple.getOffset());
    }

    windowStore.put(key, aggregatorState);
  }

//  public AggregateState getAggregateState(Integer groupId, PartitionKey partitionKey) {
//    if (aggregateStateStoreMap.get(groupId) != null) {
//      return aggregateStateStoreMap.get(groupId).get(partitionKey);
//    }
//
//    return null;
//  }
//
//  public void putAggregateState(Integer groupId, PartitionKey partitionKey, AggregateState aggregateState) {
//    aggregatesMap.get(groupId).put(partitionKey, aggregateState);
//  }

  public KeyValueStore<PartitionKey, AggregateState> getPartitions(Integer groupId) {
    return aggregateStateStoreMap.get(groupId);
  }

//  public void initPartitions(Integer groupId) {
//    aggregateStateStoreMap.put(groupId, null);
//  }

  public void purgeMessages(Integer groupId, Function2<IntermediateMessageTuple, KeyValueStore<PartitionKey, AggregateState>, Void> adjustAggregate) {
    WindowStore windowStore = windowStoreMap.get(groupId);
    KeyValueIterator<OrderedStoreKey, TimeBasedSlidingWindowAggregatorState> messagesToPurge =
        windowStore.range(new TimeKey(0L), new TimeKey(getLowerBound(groupId)));

    KeyValueStore<PartitionKey, AggregateState> partitions = aggregateStateStoreMap.get(groupId);
    while (messagesToPurge.hasNext()) {
      Entry<OrderedStoreKey, TimeBasedSlidingWindowAggregatorState> entry = messagesToPurge.next();
      for (OrderedStoreKey key : entry.getValue().getTuples()) {
        if(key != null) {
          IntermediateMessageTuple tuple = (IntermediateMessageTuple) messageStoreMap.get(groupId).get(key);
          adjustAggregate.apply(tuple, partitions);
        } else {
          System.out.println("Null Tuples.");
        }
      }
    }

    messagesToPurge.close();
  }

  public static class GroupState {
    public long lowerBound;
    public long upperBound;

    public GroupState(long lowerBound, long upperBound) {
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }
  }

  public static class AggregateState extends HashMap<Integer, Object> {

  }

  public static class PartitionKey {
    private final Object[] values;

    public PartitionKey(Object[] values) {
      this.values = values;
    }

    public static PartitionKey of(Object o1) {
      return new PartitionKey(new Object[]{o1});
    }

    public static PartitionKey of(Object o1, Object o2) {
      return new PartitionKey(new Object[]{o1, o2});
    }

    public static PartitionKey of(Object o1, Object o2, Object o3) {
      return new PartitionKey(new Object[]{o1, o2, o3});
    }

    public static PartitionKey of(Object... values) {
      return new PartitionKey(values);
    }

    public Object[] getValues() {
      return values;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this || obj instanceof PartitionKey && Arrays.equals(values, ((PartitionKey) obj).values);
    }

    @Override
    public String toString() {
      return Arrays.toString(values);
    }
  }

  public static class PartitionKeyBuilder {
    Object[] values;

    public PartitionKeyBuilder(int size) {
      values = new Object[size];
    }

    public void set(Integer index, Object value) {
      values[index] = value;
    }

    public PartitionKey build() {
      return new PartitionKey(values);
    }

    public int size() {
      return values.length;
    }
  }
}

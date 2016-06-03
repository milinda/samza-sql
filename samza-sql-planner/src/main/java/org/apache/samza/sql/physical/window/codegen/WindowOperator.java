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

import org.apache.calcite.linq4j.function.Function2;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class WindowOperator extends SimpleOperatorImpl {

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
   * Keep state of each group of window aggregates.
   */
  protected Map<Integer, GroupState> groupStateMap = new HashMap<Integer, GroupState>();

  /**
   * Aggregation results
   */
  protected Map<Integer, Map<PartitionKey, AggregateState>> aggregatesMap = new HashMap<Integer, Map<PartitionKey, AggregateState>>();

  protected WindowOperatorSpec spec;


  public WindowOperator() {
    super(null);
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
  public abstract void realProcess(Tuple ituple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception;

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    this.taskContext = taskContext;
  }

  public void setId(String id) {
    this.id = id;
  }

  public GroupState getGroupState(Integer groupId) {
    return groupStateMap.get(groupId);
  }

  public WindowStore getWindowStore(Integer groupId) {
    return windowStoreMap.get(groupId);
  }

  public MessageStore getMessageStore(Integer groupId) {
    return messageStoreMap.get(groupId);
  }

  public void initWindowStore(Integer groupId) {
    this.windowStoreMap.put(groupId, new WindowStore((KeyValueStore<OrderedStoreKey, TimeBasedSlidingWindowAggregatorState>) taskContext.getStore(String.format("wnd-store-group-%d", groupId))));
  }

  public void initMessageStore(Integer groupId) {
    this.messageStoreMap.put(groupId, (MessageStore) taskContext.getStore(String.format("msg-store-group-%d", id)));
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
    groupStateMap.get(groupId).lowerBound = timestamp;
  }

  public void updateUpperBound(Integer groupId, Long timestamp) {
    groupStateMap.get(groupId).upperBound = timestamp;
  }

  public Long getUpperBound(Integer groupId) {
    return groupStateMap.get(groupId).upperBound;
  }

  public Long getLowerBound(Integer groupId) {
    return groupStateMap.get(groupId).lowerBound;
  }

  public void initGroupState(Integer groupId, Long precededBy) {
    groupStateMap.put(groupId, new GroupState(Long.MAX_VALUE, Long.MIN_VALUE));
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

  public AggregateState getAggregateState(Integer groupId, PartitionKey partitionKey) {
    if (aggregatesMap.get(groupId) != null) {
      return aggregatesMap.get(groupId).get(partitionKey);
    }

    return null;
  }

  public void putAggregateState(Integer groupId, PartitionKey partitionKey, AggregateState aggregateState) {
    aggregatesMap.get(groupId).put(partitionKey, aggregateState);
  }

  public Map<PartitionKey, AggregateState> getPartitions(Integer groupId) {
    return aggregatesMap.get(groupId);
  }

  public void initPartitions(Integer groupId) {
    aggregatesMap.put(groupId, new HashMap<PartitionKey, AggregateState>());
  }

  public void purgeMessages(Integer groupId, Function2<IntermediateMessageTuple, Map<PartitionKey, AggregateState>, Void> adjustAggregate) {
    WindowStore windowStore = windowStoreMap.get(groupId);
    KeyValueIterator<OrderedStoreKey, TimeBasedSlidingWindowAggregatorState> messagesToPurge =
        windowStore.range(new TimeKey(0L), new TimeKey(getLowerBound(groupId)));

    Map<PartitionKey, AggregateState> partitions = aggregatesMap.get(groupId);
    while (messagesToPurge.hasNext()) {
      Entry<OrderedStoreKey, TimeBasedSlidingWindowAggregatorState> entry = messagesToPurge.next();
      for(OrderedStoreKey key : entry.getValue().getTuples()) {
        IntermediateMessageTuple tuple = (IntermediateMessageTuple) messageStoreMap.get(groupId).get(key);
        adjustAggregate.apply(tuple, partitions);
      }
    }
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

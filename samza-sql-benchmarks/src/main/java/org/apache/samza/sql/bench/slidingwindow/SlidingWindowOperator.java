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
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.operators.SimpleOperatorImpl;
import org.apache.samza.sql.physical.window.TimeBasedSlidingWindowAggregatorState;
import org.apache.samza.sql.physical.window.WindowOperatorSpec;
import org.apache.samza.sql.window.storage.TimeAndOffsetKey;
import org.apache.samza.sql.window.storage.TimeKey;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.sql.LongOffset;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SlidingWindowOperator extends SimpleOperatorImpl {

  private static Logger log = LoggerFactory.getLogger(SlidingWindowOperator.class);

  private static final Long WND_LOWER_BOUND_DEFAULT = 9223372036854775807L;
  private static final Long WND_UPPER_BOUND_DEFAULT = -1L;

  private final WindowOperatorSpec spec;

  protected Map<Integer, KeyValueStore<TimeAndOffsetKey, IntermediateMessageTuple>> messageStoreMap =
      new HashMap<Integer, KeyValueStore<TimeAndOffsetKey, IntermediateMessageTuple>>();

  protected Map<Integer, KeyValueStore<TimeKey, TimeBasedSlidingWindowAggregatorState>> windowStoreMap =
      new HashMap<Integer, KeyValueStore<TimeKey, TimeBasedSlidingWindowAggregatorState>>();

  protected KeyValueStore<String, Long> groupStateStore;

  protected Map<Integer, KeyValueStore<PartitionKey, Object[]>> aggregateStateStoreMap =
      new HashMap<Integer, KeyValueStore<PartitionKey, Object[]>>();

  private final int numberOfGroups;

  private final Function1<Object[], Long> timestampSelector;

  private final Map<Integer, Function0<Long>> windowSizeSelectors =
      new HashMap<Integer, Function0<Long>>();

  private final Map<Integer, Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Void>> expiredMessagePurgers =
      new HashMap<Integer, Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Void>>();

  private final Map<Integer, Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Object[]>> updateAggregateAndReturnResult =
      new HashMap<Integer, Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Object[]>>();


  public SlidingWindowOperator(WindowOperatorSpec spec,
                               int numberOfGroups,
                               Function1<Object[], Long> timestampSelector,
                               Map<Integer, Function0<Long>> windowSizeSelectors,
                               Map<Integer, Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Void>> expiredMessagePurgers,
                               Map<Integer, Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Object[]>> updateAggregateAndReturnResult) {
    super(spec);
    this.spec = spec;
    this.numberOfGroups = numberOfGroups;
    this.timestampSelector = timestampSelector;
    this.windowSizeSelectors.putAll(windowSizeSelectors);
    this.expiredMessagePurgers.putAll(expiredMessagePurgers);
    this.updateAggregateAndReturnResult.putAll(updateAggregateAndReturnResult);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) throws Exception {
    groupStateStore = (KeyValueStore<String, Long>) context.getStore("group-state");
    for (int i = 0; i < numberOfGroups; i++) {
      windowStoreMap.put(i,
          (KeyValueStore<TimeKey, TimeBasedSlidingWindowAggregatorState>) context.getStore(String.format("wnd-store-group-%d", i)));
      messageStoreMap.put(i,
          (KeyValueStore<TimeAndOffsetKey, IntermediateMessageTuple>) context.getStore(String.format("msg-store-group-%d", i)));
      aggregateStateStoreMap.put(i,
          (KeyValueStore<PartitionKey, Object[]>) context.getStore(String.format("aggstatestore-group-%s", i)));
    }
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
  protected void realProcess(Tuple ituple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if (!(ituple instanceof IntermediateMessageTuple)) {
      throw new SamzaException("Unsupported tuple type " + ituple.getClass());
    }

    IntermediateMessageTuple tuple = (IntermediateMessageTuple) ituple;
    final Object[] tupleContent = tuple.getContent();
    Long timestamp = timestampSelector.apply(tupleContent);

    if (isReplay(timestamp, (LongOffset) tuple.getOffset())) {
      return;
    }

    if (timestamp > this.getUpperBound(0)) {
      this.updateUpperBound(0, timestamp);
    }

    if (this.getLowerBound(0).equals(WND_LOWER_BOUND_DEFAULT)) {
      this.updateLowerBound(0, timestamp);
    } else {
      Long newLowerBound = timestamp - windowSizeSelectors.get(0).apply();
      if (newLowerBound > 0) {
        this.updateLowerBound(0, newLowerBound);
      }
    }

    addMessageToWindow(0, timestamp, tuple);

    purgeExpiredMessagesFromWindow(0, expiredMessagePurgers.get(0));

    Object[] result = updateAggregateAndReturnResult.get(0).apply(tupleContent, aggregateStateStoreMap.get(0));

    try {
      collector.send(IntermediateMessageTuple.fromTupleAndContent(tuple, result, this.getOutputStreamName()));
    } catch (Exception e) {
      throw new SamzaException("Sending output failed.", e);
    }

    messageStoreMap.get(0).put(new TimeAndOffsetKey(timestamp, tuple.getOffset()), tuple);
  }

  public void updateLowerBound(Integer groupId, Long timestamp) {
    groupStateStore.put("lower-" + groupId, timestamp);
  }

  public void updateUpperBound(Integer groupId, Long timestamp) {
    groupStateStore.put("upper-" + groupId, timestamp);
  }

  public Long getUpperBound(Integer groupId) {
    Long upper = groupStateStore.get("upper-" + groupId);
    return upper == null ? WND_UPPER_BOUND_DEFAULT : upper;
  }

  public Long getLowerBound(Integer groupId) {
    Long lower = groupStateStore.get("lower-" + groupId);
    return lower == null ? 9223372036854775807L : lower;
  }

  public void addMessageToWindow(Integer groupId, Long tupleTimestamp, IntermediateMessageTuple tuple) {
    TimeKey key = new TimeKey(tupleTimestamp);
    KeyValueStore<TimeKey, TimeBasedSlidingWindowAggregatorState> windowStore = windowStoreMap.get(groupId);
    TimeBasedSlidingWindowAggregatorState aggregatorState = windowStore.get(key);

    if (aggregatorState != null) {
      aggregatorState.addTuple(tupleTimestamp, tuple.getOffset());
    } else {
      aggregatorState = new TimeBasedSlidingWindowAggregatorState(tupleTimestamp, tuple.getOffset());
    }

    windowStore.put(key, aggregatorState);
  }

  public void purgeExpiredMessagesFromWindow(Integer groupId, Function2<Object[], KeyValueStore<PartitionKey, Object[]>, Void> adjustAggregate) {
    KeyValueStore<TimeKey, TimeBasedSlidingWindowAggregatorState> windowStore = windowStoreMap.get(groupId);
    KeyValueIterator<TimeKey, TimeBasedSlidingWindowAggregatorState> messagesToPurge =
        windowStore.range(new TimeKey(0L), new TimeKey(getLowerBound(groupId)));

    KeyValueStore<PartitionKey, Object[]> partitions = aggregateStateStoreMap.get(groupId);
    while (messagesToPurge.hasNext()) {
      Entry<TimeKey, TimeBasedSlidingWindowAggregatorState> entry = messagesToPurge.next();
      for (TimeAndOffsetKey key : entry.getValue().getTuples()) {
        if (key != null) {
          IntermediateMessageTuple tuple = messageStoreMap.get(groupId).get(key);
          adjustAggregate.apply(tuple.getContent(), partitions);
        } else {
          if (log.isDebugEnabled()) {
            log.debug("Null key in time based sliding window aggregate state.");
          }
        }
      }
    }

    messagesToPurge.close();
  }

  public boolean isReplay(Long tupleTimestamp, LongOffset tupleOffset) {
    return messageStoreMap.get(new TimeAndOffsetKey(tupleTimestamp, tupleOffset)) != null;
  }

  public void addToMessageStore(Integer groupId, TimeAndOffsetKey key, IntermediateMessageTuple t) {
    messageStoreMap.get(groupId).put(key, t);
  }

  public KeyValueStore<PartitionKey, Object[]> getPartitions(Integer groupId) {
    return aggregateStateStoreMap.get(groupId);
  }

  public String getId() {
    return spec.getId();
  }

  public EntityName getOutputStreamName() {
    return spec.getOutputName();
  }
}

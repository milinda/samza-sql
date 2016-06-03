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

package org.apache.samza.sql.physical.window;

import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.window.storage.MessageStore;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.sql.window.storage.TimeAndOffsetKey;
import org.apache.samza.sql.window.storage.TimeKey;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.sql.SimpleMessageCollector;

import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

public class SlidingWindowGroup {
  /**
   * Keep windows within the retention period
   */
  private WindowStore windowStore;

  private MessageStore messageStore;

  /**
   * Constants used in aggregate expressions
   */
  private final List<Expressions> constants;

  /**
   * Stream window group id
   */
  private final String id;

  /**
   * Field where this stream is ordered
   */
  private final int orderFieldIndex;

  private final SqlTypeName orderFieldTypeName;

  /**
   * Type of the input tuple
   */
  private final RelDataType inputType;

  /**
   * Type of the output tuple
   */
  private final RelDataType outputType;

  /**
   * Lower bound of the current sliding window in milli seconds
   */
  private long lowerBoundMillis = Long.MAX_VALUE;

  /**
   * Upper bound of the current sliding window in milli seconds. In streaming upper bound will always be the max
   * timestamp the operator has seen.
   */
  private long upperBoundMillis = Long.MIN_VALUE;

  private final long windowPrecededByMills;

  public SlidingWindowGroup(String id, long windowPrecededByMills, List<Expressions> constants, RelCollation orderKeys, RelDataType inputType,
                            RelDataType outputType) {
    this.id = id;
    this.windowPrecededByMills = windowPrecededByMills;
    this.constants = constants;

    if (orderKeys.getFieldCollations().size() != 1) {
      throw new SamzaException("For time based sliding windows, ORDER BY should have exactly one expression. Actual collation is " + orderKeys);
    }

    RelFieldCollation fieldCollation = orderKeys.getFieldCollations().get(0);
    RelFieldCollation.Direction orderFieldDirection = fieldCollation.getDirection();

    if (orderFieldDirection != RelFieldCollation.Direction.ASCENDING && orderFieldDirection != RelFieldCollation.Direction.STRICTLY_ASCENDING) {
      throw new SamzaException("For time based sliding window, we expect increasing time column");
      // TODO: is increasing this necessary
    }

    RelDataTypeField orderFieldType = inputType.getFieldList().get(fieldCollation.getFieldIndex());
    this.orderFieldTypeName = orderFieldType.getType().getSqlTypeName();
    if (orderFieldTypeName != SqlTypeName.DATE && orderFieldTypeName != SqlTypeName.TIME && orderFieldTypeName != SqlTypeName.TIMESTAMP) {
      throw new SamzaException("For time based sliding window, order field should be one of the following types: DATE, TIME, TIMESTAMP");
    }

    this.orderFieldIndex = orderFieldType.getIndex();

    this.inputType = inputType;
    this.outputType = outputType;
  }

  public void init(TaskContext taskContext) {
    this.windowStore = new WindowStore(
        (KeyValueStore<OrderedStoreKey, TimeBasedSlidingWindowAggregatorState>) taskContext.getStore("wnd-store-" + id));
    this.messageStore = (MessageStore) taskContext.getStore("wnd-msg-" + id);
  }

  public IntermediateMessageTuple send(IntermediateMessageTuple t, SimpleMessageCollector collector) {

    /*
     * 09-26-2015 - Milinda Pathirage
     * ==============================
     * - Handling late arrivals is not clear yet. So the basic implementation discard late arrivals.
     * - Output tuple contains input tuple + aggregated values
     * - TODO: Implement support for count and sum
     * - TODO: lower and upper bounds should be updated as we go along (use current tuple's timestamp or max time stamp as a reference)
     * - TODO: we need to generate expressions to update lower bound and upper bound
     * - TODO: Logic can be discovered by writing a simple test program calculating time based sliding window
     */

    // TODO: We need to process this and compute on it to check whether message is in the window
    Object tupleTimeObj = t.getContent()[orderFieldIndex];
    Long tupleTimestamp;
    switch (orderFieldTypeName) {
      case DATE:
        tupleTimestamp = ((Date) tupleTimeObj).getTime();
        break;
      case TIME:
        tupleTimestamp = ((Time) tupleTimeObj).getTime();
        break;
      case TIMESTAMP:
        tupleTimestamp = (Long) tupleTimeObj;
        break;
      default:
        throw new SamzaException("Order fields type should be a time related type.");
    }

    if(isReplay(tupleTimestamp, t)){
      /* TODO: Handling replay may not be simple like this. We may need to persist bounds as well or update bounds from replayed messages. */
      return null;
    }

    /* Persisting the tuple to handle replay, etc. */
    messageStore.put(new TimeAndOffsetKey(tupleTimestamp, t.getOffset()), t);

    if (tupleTimestamp > upperBoundMillis) {
      /* Sliding window's upper bound is calculated based on timestamps of the incoming tuples. If messages get delayed
       * and we somehow receive a message from future with respect to the current flow, some message may get lost
       * because their timestamp is lower than the lower bound calculated based on upper bound.
       * TODO: Explore how to utilize techniques (punctuations, low water marks, heart beats) from literature to solve this case. */
      upperBoundMillis = tupleTimestamp;
    }

    if (upperBoundMillis > windowPrecededByMills) {
      lowerBoundMillis = upperBoundMillis - windowPrecededByMills;
    } else {
      if (tupleTimestamp < lowerBoundMillis) {
        lowerBoundMillis = tupleTimestamp;
      }
    }

    /*
      if(tupleTimestamp > getUpperBound(i)) {
        updateUpperBound(i, tupleTimestamp);
      }

      if (getLowerBound(i) == Long.MAX_VALUE) {
        updateLowerBound(i, tupleTimestamp);
      } else {
        Long diff = calculateLowerBound(getUpperBound(i));
        if ( diff > 0 ) {
          updateLowerBound(i, diff);
        }
      }

      KeyValueIterator<OrderedStoreKey, TimeBasedSlidingWindowAggregateState> messagesToPurge = windowStore.range(new TimeKey(0L), new TimeKey(lowerBound));

      // Lets say aggregate is a count and we have partitions. Lets also assume that we only supports monotonic aggregates
      // so for each group, we have to initialize aggregate state
      // AggregateState {Map<aggId or agg Name, aggValue>}
      // Map<GroupId, Map<Partition, AggState>> aggs

      if(getPartitions(gId) == null) {
        initPartitions(gId);
      }

      Map<Partition, AggState> aggState = getAggregateState(gId);

      forAll(messagesToPurge) {
        Partition pkey = createPartitionFromTuple(pt);
        removeValue(aggState.get(pKey), pt);
      }

      Partion pkey = createPartitionKeyFromTuple(t);
      AggState st = aggState.get(pkey);

      addCurrentTuple(st, t);

      emitResult(st, t);

      purgeMessages();



     */

    KeyValueIterator<OrderedStoreKey, TimeBasedSlidingWindowAggregatorState> messagesToPurge = windowStore.range(new TimeKey(0L), new TimeKey(lowerBoundMillis));
    List<OrderedStoreKey> messageKeysToPurge = new ArrayList<OrderedStoreKey>();

    while(messagesToPurge.hasNext()){
      Entry<OrderedStoreKey, TimeBasedSlidingWindowAggregatorState> message = messagesToPurge.next();
      // TODO: Update aggregates

      messageKeysToPurge.add(message.getKey());
    }

    for(OrderedStoreKey key: messageKeysToPurge){
      windowStore.delete(key);
    }

    if(tupleTimestamp >= lowerBoundMillis){
      OrderedStoreKey timeKey = new TimeKey(tupleTimestamp);
      TimeBasedSlidingWindowAggregatorState state = windowStore.get(timeKey);
      if(state != null) {
       // state.addTuple(tupleTimestamp, t.getOffset());
        // TODO: Do we need to update bounds as well
        windowStore.put(timeKey, state);
      } else {
        // TODO: Dow we really need offset
        windowStore.put(timeKey, null);
      }

      // Calcite has standard aggregator implementors. What I need to do is select the proper partition and invoke the aggregator accorss that partition
      // We need to decide how we store partitions of window
    }

    return null;
  }

  private boolean isReplay(long tupleTimestamp, IntermediateMessageTuple tuple) {
    return messageStore.get(new TimeAndOffsetKey(tupleTimestamp, tuple.getOffset())) != null;
  }

}

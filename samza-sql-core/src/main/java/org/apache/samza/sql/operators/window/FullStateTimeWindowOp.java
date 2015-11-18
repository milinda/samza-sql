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

package org.apache.samza.sql.operators.window;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Stream;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.exception.OperatorException;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.sql.window.storage.Range;
import org.apache.samza.sql.window.storage.TimeAndOffsetKey;
import org.apache.samza.sql.window.storage.TimeKey;
import org.apache.samza.sql.window.storage.WindowOutputStream;
import org.apache.samza.sql.window.storage.WindowState;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.system.sql.LongOffset;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;


/**
 * This class implements a full state time-based window operator.
 */
public class FullStateTimeWindowOp extends FullStateWindowOp implements FullStateWindow<Long> {

  private final String outputStreamName;

  public FullStateTimeWindowOp(WindowOpSpec spec) {
    this(spec, null);
  }

  public FullStateTimeWindowOp(WindowOpSpec spec, OperatorCallback callback) {
    super(spec, callback);
    this.outputStreamName = String.format("wnd-outstrm-%s", wndId);
  }

  //Ctor for fixed length time window, w/ default retention policy, message store, and timestamp
  public FullStateTimeWindowOp(String wndId, int size, String inputStrm, String outputEntity) {
    this(new WindowOpSpec(wndId, EntityName.getStreamName(inputStrm), EntityName.getStreamName(outputEntity), size));
  }

  //Ctor for fixed length time window, w/ default retention policy, message store, and timestamp
  public FullStateTimeWindowOp(String wndId, int size, String inputStrm, String outputEntity, OperatorCallback callback) {
    // TODO Auto-generated constructor stub
    this(new WindowOpSpec(wndId, EntityName.getStreamName(inputStrm), EntityName.getStreamName(outputEntity), size),
        callback);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(Config config, TaskContext context) throws Exception {
    super.init(config, context);
    this.outputStream =
        new WindowOutputStream<OrderedStoreKey>((Stream<OrderedStoreKey>) context.getStore(this.outputStreamName),
            EntityName.getStreamName(this.outputStreamName), this.getSpec().getMessageStoreSpec());
  }

  @Override
  protected void addMessage(Tuple tuple) throws Exception {
    // TODO: 1. Check whether the message selector is disabled for this window operator
    //    1. If yes, log a warning and throws exception
    if (this.isInputDisabled()) {
      throw OperatorException.getInputDisabledException(String.format(
          "The input to this window operator is disabled. Operator ID: %s", this.getSpec().getId()));
    }
    // 2. Check whether the message is out-of-retention
    // TODO:   1. If yes, send the message to error topic and return
    if (this.isExpired(tuple)) {
      throw OperatorException.getMessageTooOldException(String.format("Message is out-of-retention. Operator ID: %s",
          this.getSpec().getId()));
    }
    // 3. Check to see whether the message is a replay that we have already sent result for
    //    1. If yes, return as NOOP
    //    2. Otherwise, continue
    if (this.isRepeatedMessage(tuple)) {
      return;
    }
    // 4. Add incoming message to all windows that includes it and add those windows to pending output list
    updateWindows(tuple, this.getAllWindows(tuple));
    // 5. For all windows in pending output list, update the window operator's output stream
    refresh();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected WindowOutputStream<OrderedStoreKey> getResult() {
    return this.outputStream;
  }

  @Override
  protected void flush() throws Exception {
    // clear the current window output, purge the out-of-retention windows,
    // and flush the wndStore and messageStore
    this.outputStream.clear();
    // TODO: depending on whether we delay the KV-store deletion in a LoggedStore, the order of purge() and flush() may need to be changed.
    this.purge();
    // NOTE: the following flush sequence can result in the scenario that some messages persisted in MessageStore have not been output yet.
    // The recovery procedure in the window operator should re-construct the pending output per window list, when restart the container.
    // TODO: we need an efficient way to identify that from which offset that we need to re-construct some pending output windows.
    this.wndStore.flush(this.pendingFlushWindows);
    this.pendingFlushWindows.clear();
    this.messageStore.flush();
  }

  @Override
  protected void refresh() {
    for (OrderedStoreKey key : this.pendingOutputPerWindow.keySet()) {
      // for each window w/ pending output, check to see whether we need to emit the corresponding window outputs
      //       1. If the window is the current window, check with early emission policy to see whether we need to flush aggregated result
      //          1. If yes, add this window to pending flush list and continue
      //       2. For any window, check the full size policy to see whether we need to send out the window output
      //          1. If yes, add this window to pending flush list and continue
      //       3. For a past window, check with late arrival policy to see whether we need to send out past window outputs
      //          1. If yes, add this window to pending flush list and continue
      if (hasScheduledOutput(key) || hasFullSizeOutput(key) || hasLateArrivalOutput(key)) {
        addToOutputs(key);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void addToOutputs(OrderedStoreKey key) {
    Map<OrderedStoreKey, Tuple> pendingOutputPerWindow = this.pendingOutputPerWindow.get(key);
    for (Tuple tuple : pendingOutputPerWindow.values()) {
      ((WindowOutputStream<OrderedStoreKey>) this.outputStream).put(new TimeAndOffsetKey(
          this.getMessageTimeNano(tuple), tuple.getOffset()), tuple);
    }
    this.pendingOutputPerWindow.remove(key);
    this.pendingFlushWindows.add(key);
  }

  private OrderedStoreKey getMessageStoreKey(Tuple tuple) {
    return this.messageStore.getKey(new TimeAndOffsetKey(this.getMessageTimeNano(tuple), tuple.getOffset()), tuple);
  }

  // TODO [Milinda] This is the place where we need to evaluate aggregate functions. But need to move it to separate class
  private void updateWindows(Tuple tuple, List<OrderedStoreKey> wndKeys) {
    boolean addedToPendingOutput = false;
    for (OrderedStoreKey key : wndKeys) {
      WindowState wnd = this.wndStore.get(key);
      // For full state window, no need to update the aggregated value, only update the last offset
      // TODO: to reduce the update traffic to window store, this last offset can be delayed to flush until the corresponding window output is sent
      wnd.setLastOffset(tuple.getOffset());
      this.wndStore.put(key, wnd);
      // Save message to the message store
      addMsgToStore(tuple);
      if (!addedToPendingOutput) {
        // If the message has not been sent to window output
        if (!this.pendingOutputPerWindow.containsKey(key)) {
          // add to pending output map
          this.pendingOutputPerWindow.put(key, new LinkedHashMap<OrderedStoreKey, Tuple>());
        }
        // add to each window's pending output queues
        Map<OrderedStoreKey, Tuple> pendingOutput = this.pendingOutputPerWindow.get(key);
        pendingOutput.put(new TimeAndOffsetKey(getMessageTimeNano(tuple), tuple.getOffset()), tuple);
        addedToPendingOutput = true;
      }
    }
  }

  private void addMsgToStore(Tuple tuple) {
    // Idempotent add operation
    if (this.messageStore.get(this.messageStore.getKey(getMessageStoreKey(tuple), tuple)) == null) {
      this.messageStore.put(this.messageStore.getKey(getMessageStoreKey(tuple), tuple), tuple);
    }

  }

  private List<OrderedStoreKey> getAllWindows(Tuple tuple) {
    List<OrderedStoreKey> wndKeys = new ArrayList<OrderedStoreKey>();
    long msgTime = this.getMessageTimeNano(tuple);
    Entry<OrderedStoreKey, WindowState> firstWnd = getFirstWnd();
    // assuming this is a fixed size window, the windows that include the msgTime will be computed based on the offset in time
    // from the firstWnd and the corresponding widow size
    long stepSize = this.getStepSizeNano();
    long size = this.getSizeNano();
    // The start time range for all windows that may include the msgTime is: (msgTime - size, msgTime] == [msgTime - size + 1, msgTime]
    // Based on the initialized window boundary, we need to search for all windows in this range and if there are missing windows, add them.
    long startWnd = 0;
    if (firstWnd == null) {
      startWnd = msgTime;
    } else {
      long offsetTime = msgTime - size + 1 - firstWnd.getValue().getStartTimeNano();
      long numOfWnds = offsetTime / stepSize;
      if (offsetTime < 0) {
        numOfWnds--;
      }
      startWnd = firstWnd.getValue().getStartTimeNano() + numOfWnds * stepSize;
    }
    while (startWnd < msgTime) {
      TimeKey wndKey = new TimeKey(startWnd);
      if (this.wndStore.get(wndKey) == null) {
        // If the window if missing, open the new window and save to the store
        long startTime = wndKey.getTimeNano();
        long endTime = startTime + size;
        this.wndStore.put(wndKey, new WindowState(tuple.getOffset(), startTime, endTime));
      }
      wndKeys.add(wndKey);
      startWnd += stepSize;
    }
    return wndKeys;
  }

  private long getSizeNano() {
    return this.getSpec().getNanoTime(this.getSpec().getSize());
  }

  private long getStepSizeNano() {
    return this.getSpec().getNanoTime(this.getSpec().getStepSize());
  }

  private long getMessageTimeNano(Tuple tuple) {
    if (this.getSpec().getTimestampField() != null) {
      return this.getSpec()
          .getNanoTime(tuple.getMessage().getFieldData(this.getSpec().getTimestampField()).longValue());
    }
    // TODO: need to get the event time in broker envelope
    return tuple.getCreateTimeNano();
  }

  private Range<OrderedStoreKey> getMessageKeyRangeByTime(Range<Long> timeRange) {
    // TODO: the range may need to be adjusted to reflect retention policies
    OrderedStoreKey minKey = new TimeAndOffsetKey(timeRange.getMin(), LongOffset.getMinOffset());
    // Set to the next nanosecond w/ minimum offset since the right boundary is exclusive
    // TODO: need to work on Offset that is not LongOffset
    OrderedStoreKey maxKey = new TimeAndOffsetKey(timeRange.getMax() + 1, LongOffset.getMinOffset());
    return Range.between(minKey, maxKey);
  }

  @Override
  public KeyValueIterator<OrderedStoreKey, Tuple> getMessages(Range<Long> timeRange,
      List<Entry<String, Object>> filterFields) {
    Range<OrderedStoreKey> keyRange = getMessageKeyRangeByTime(timeRange);
    // This is to get the range from the messageStore and possibly apply filters in the key or the value
    return this.messageStore.getMessages(keyRange, filterFields);
  }

  @Override
  public KeyValueIterator<OrderedStoreKey, Tuple> getMessages(Range<Long> timeRange) {
    // TODO Auto-generated method stub
    return this.getMessages(timeRange, new ArrayList<Entry<String, Object>>());
  }

  @Override
  protected boolean isRepeatedMessage(Tuple msg) {
    // TODO: this needs to check the message store to see whether the message is already there.
    // TODO: this also needs to check against the wndStore lastOffsets to make sure that the message output is sent out
    return this.messageStore.get(this.getMessageStoreKey(msg)) != null;
  }

  @Override
  protected void purge() {
    // check all wnds in the wndStore and all messages in the messageStore to see whether we need to purge/remove out-of-retention windows
    KeyValueIterator<OrderedStoreKey, WindowState> iter = this.wndStore.all();
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    while (iter.hasNext()) {
      Entry<OrderedStoreKey, WindowState> entry = iter.next();
      if (this.isExpired(entry.getValue())) {
        this.wndStore.delete(entry.getKey());
        if (this.pendingFlushWindows.contains(entry.getKey())) {
          this.pendingFlushWindows.remove(entry.getKey());
        }
        if (maxTime < entry.getValue().getEndTimeNano()) {
          maxTime = entry.getValue().getEndTimeNano();
        }
        if (minTime > entry.getValue().getStartTimeNano()) {
          minTime = entry.getValue().getStartTimeNano();
        }
      }
    }

    // now purge message store
    // Set to the next nanosecond w/ minimum offset since the right boundary is exclusive
    OrderedStoreKey minKey = new TimeAndOffsetKey(minTime, LongOffset.getMinOffset());
    OrderedStoreKey maxKey = new TimeAndOffsetKey(maxTime + 1, LongOffset.getMinOffset());
    Range<OrderedStoreKey> range = Range.between(minKey, maxKey);
    this.messageStore.purge(range);
  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Tuple ituple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }
}

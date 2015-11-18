/**
 * Copyright (C) 2015 Trustees of Indiana University
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.samza.sql.operators.window2;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.exception.OperatorException;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.sql.window.storage.TimeAndOffsetKey;
import org.apache.samza.sql.window.storage.TimeKey;
import org.apache.samza.sql.window.storage.WindowState;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

import java.util.ArrayList;
import java.util.List;

public class TimeBasedWindowOp extends WindowOp {

  private final FixedWindowSpec spec;

  public TimeBasedWindowOp(FixedWindowSpec spec, OperatorCallback callback) {
    super(spec, callback);
    this.spec = spec;
  }

  public TimeBasedWindowOp(FixedWindowSpec spec) {
    super(spec);
    this.spec = spec;
  }

  @Override
  protected boolean isExpired(WindowState window) {
    return false;
  }

  @Override
  public boolean isExpired(Tuple tuple) {
    return false;
  }

  @Override
  public boolean isReplay(Tuple tuple) {
    return false;
  }

  @Override
  public List<OrderedStoreKey> getWindows(Tuple tuple) {
    List<OrderedStoreKey> windowKeys = new ArrayList<OrderedStoreKey>();

    // Get the current window
    Entry<OrderedStoreKey, WindowState> latestKnownWindow = getFirstWindow();

    long messageTime = getMessageTimeNano(tuple);
    long windowSize = getWindowSizeNano();
    long hopSize = getHopSizeNano();

    long lowerBoundOfWindowStartTime = 0;

    if (latestKnownWindow == null) {
      // new window
      lowerBoundOfWindowStartTime = messageTime;
    } else {
      long offsetTime = messageTime - latestKnownWindow.getValue().getStartTimeNano() - windowSize + 1;
      long numberOfWindows = offsetTime / hopSize;

      if (offsetTime < 0) {
        numberOfWindows--;
      }

      lowerBoundOfWindowStartTime = latestKnownWindow.getValue().getStartTimeNano() + numberOfWindows * hopSize;
    }

    while (lowerBoundOfWindowStartTime <= messageTime) {  // equal case to handle the new window
      TimeKey windowKey = new TimeKey(lowerBoundOfWindowStartTime);     // TODO: this will only work for aggregate, but offset is missing from key?

      if (windowStore.get(windowKey) == null) {
        // Window is missing, open a new window and save it to the store
        long startTime = windowKey.getTimeNano();
        long endTime = startTime + windowSize;
        windowStore.put(windowKey, new WindowState(tuple.getOffset(), startTime, endTime));
      }

      windowKeys.add(windowKey);

      // advance the window
      lowerBoundOfWindowStartTime += hopSize;
    }

    return windowKeys;
  }

  @Override
  protected void flush() throws Exception {
    // TODO: flush implementation
  }

  @Override
  protected void refresh() throws Exception {
    // TODO: refresh implementation
  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Tuple tuple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if (isInputDisabled()) {
      throw OperatorException.getInputDisabledException(String.format(
          "The input to this window operator is disabled. Operator ID: %s", this.getSpec().getId()));
    }

    if (isExpired(tuple)) {
      // TODO: send to the error topic
      throw OperatorException.getMessageTooOldException(String.format("Message is out-of-retention. Operator ID: %s",
          this.getSpec().getId()));
    }

    if (isReplay(tuple)) {
      return;
    }

    List<OrderedStoreKey> windowsTupleContributesTo = getWindows(tuple);

    // TODO: update all windows
    updateWindows(tuple, windowsTupleContributesTo);

    // send pending window updates to downstream operators
    refresh();
  }

  private void updateWindows(Tuple tuple, List<OrderedStoreKey> windowKeys) {
    for(OrderedStoreKey windowKey : windowKeys) {
      WindowState window = this.windowStore.get(windowKey);
      window.setLastOffset(tuple.getOffset());
      this.windowStore.put(windowKey, window);
      addMessageToStore(tuple);



    }
  }

  private long getMessageTimeNano(Tuple tuple) {
    String timestampField = spec.getTimestampField();
    if (timestampField != null) {
      return spec.getNanoTime(tuple.getMessage().getFieldData(timestampField).longValue());
    }

    return tuple.getCreateTimeNano();
  }

  private long getWindowSizeNano() {
    return spec.getNanoTime(spec.getSize());
  }

  private long getHopSizeNano() {
    return spec.getNanoTime(spec.getHopSize());
  }

  private OrderedStoreKey getMessageStoreKey(Tuple tuple) {
    return messageStore.getKey(new TimeAndOffsetKey(getMessageTimeNano(tuple), tuple.getOffset()), tuple);
  }

  private void addMessageToStore(Tuple tuple) {
    // Idempotent add operation
    if (messageStore.get(messageStore.getKey(getMessageStoreKey(tuple), tuple)) == null) {
      messageStore.put(messageStore.getKey(getMessageStoreKey(tuple), tuple), tuple);
    }

  }
}

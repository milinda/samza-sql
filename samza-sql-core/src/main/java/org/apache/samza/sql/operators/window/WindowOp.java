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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.operators.SimpleOperatorImpl;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.sql.window.storage.WindowOutputStream;
import org.apache.samza.sql.window.storage.WindowState;
import org.apache.samza.sql.window.storage.WindowStore;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;


/**
 * This abstract class is the base for all window operators.
 */
public abstract class WindowOp extends SimpleOperatorImpl {

  /**
   * The specification object for this window operator
   */
  protected final WindowOpSpec spec;

  /**
   * The unique ID for this window operator
   */
  protected final String wndId;

  /**
   * The {@link org.apache.samza.sql.operators.window.RetentionPolicy} for this window operator
   */
  protected final RetentionPolicy retention;

  /**
   * This is the window store that keeps all window states in retention. The underlying store should be a logged store to ensure recovery.
   */
  protected WindowStore wndStore;

  /**
   * This is the flag to indicate whether the input to this window operator is disabled or not
   */
  protected boolean isInputDisabled = false;

  /**
   * This is the store for all outputs from all windows that are ready to be sent or retrieved from the window operator
   */
  @SuppressWarnings("rawtypes")
  protected WindowOutputStream outputStream;

  /**
   * This is the store for outputs per {@link org.apache.samza.sql.window.storage.OrderedStoreKey} that are are not ready to be sent or retrieved from the window operator
   */
  protected Map<OrderedStoreKey, Map<OrderedStoreKey, Tuple>> pendingOutputPerWindow;

  /**
   * This is the list of windows that has updated results in the output to be sent/retrieved recently.
   */
  protected Set<OrderedStoreKey> pendingFlushWindows;

  WindowOp(WindowOpSpec spec, OperatorCallback callback) {
    super(spec, callback);
    this.spec = spec;
    this.wndId = this.spec.getId();
    this.retention = this.spec.getRetention();
  }

  @Override
  public WindowOpSpec getSpec() {
    return this.spec;
  }

  @SuppressWarnings({ "unchecked" })
  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // TODO: One optimization to the following store is that this.wndStore can implements a delayed-write cache that does not generate 1 writes to the changelog per window update.
    // Instead, the wndStore can batch the writes and flush out the updates to changelog periodically (i.e. everytime the corresponding window output is flushed.)
    this.wndStore =
        new WindowStore((KeyValueStore<OrderedStoreKey, WindowState>) context.getStore("wnd-store-" + this.wndId));
    this.pendingOutputPerWindow = new HashMap<OrderedStoreKey, Map<OrderedStoreKey, Tuple>>();
    this.pendingFlushWindows = new HashSet<OrderedStoreKey>();
  }

  /**
   * Public API method for {@code WindowOp} to add a message to the window operator
   *
   * @param tuple The incoming message
   * @throws Exception Throws Exception if add message failed
   */
  protected abstract void addMessage(Tuple tuple) throws Exception;

  /**
   * Public API method to get the window output result
   *
   * @return The window output to be sent to the next operator; or the system output stream
   */
  @SuppressWarnings("rawtypes")
  protected abstract WindowOutputStream getResult();

  /**
   * Public API method to flush the window stores and clear the pending outputs
   *
   * @throws Exception Throws Exception if failed to flush the window states
   */
  protected abstract void flush() throws Exception;

  /**
   * Public API method to allow the window operator update the output streams
   *
   * @throws Exception Throws Exception if failed to update the output stream
   */
  protected abstract void refresh() throws Exception;

  /**
   * Default internal method to access {@code isInputDisabled} member variable
   *
   * @return The member variable {@code isInputDisabled}
   */
  protected boolean isInputDisabled() {
    return this.isInputDisabled;
  }

  protected Entry<OrderedStoreKey, WindowState> getFirstWnd() {
    KeyValueIterator<OrderedStoreKey, WindowState> wndIter = this.wndStore.all();
    while (wndIter.hasNext()) {
      Entry<OrderedStoreKey, WindowState> wnd = wndIter.next();
      if (!this.isExpired(wnd.getValue())) {
        return wnd;
      }
    }
    return null;
  }

  protected boolean isExpired(WindowState wnd) {
    //TODO: implement checking expiration of the wnd
    return false;
  }

  protected boolean isExpired(Tuple msg) {
    //TODO: implement checking expiration of the msg
    return false;
  }

  /**
   * The internal API method to check whether the incoming message is an repeated/already processed (i.e. included in the output)
   *
   * @param msg The incoming message
   * @return True if the incoming message is a repeated one; otherwise, return false;
   */
  protected abstract boolean isRepeatedMessage(Tuple msg);

  /**
   * The internal API method to check and cleanup all expired windows and messages from the window stores.
   */
  protected abstract void purge();

  /**
   * The internal API method to check whether the window corresponding to {@code key} should have a late-arrival output or not
   *
   * @param key The window key to examine for late arrival output
   * @return True if there is late arrival output to be sent out for this window {@code key}; otherwise, return false
   */
  protected boolean hasLateArrivalOutput(OrderedStoreKey key) {
    // TODO: add implementation of late arrival policy checks here
    // For each pending output window, there must be a state associated showing when the last output happened, whether this is a late arrival or not
    return false;
  }

  /**
   * The internal API method to check whether the window corresponding to {@code key} should have a full-size output or not
   *
   * @param key The window key to examine for full-size output
   * @return True if there is a full-size output to be sent out for this window {@code key}; otherwise, return false
   */
  protected boolean hasFullSizeOutput(OrderedStoreKey key) {
    // TODO: add implementation of full size output policy checks here
    // For each pending output window, there must be a state associated showing when the last update happened, whether this is triggering a window full event or not
    return false;
  }

  /**
   * The internal API method to check whether the window corresponding to {@code key} should have a scheduled output or not
   *
   * @param key The window key to examine for scheduled output
   * @return True if there is any scheduled output to be sent out for this wind {@code key}; otherwise, return false;
   */
  protected boolean hasScheduledOutput(OrderedStoreKey key) {
    // TODO: add implementation of scheduled output policy checks here
    // For each pending output window, there must be a state associated showing when the last output happened
    return false;
  }

}

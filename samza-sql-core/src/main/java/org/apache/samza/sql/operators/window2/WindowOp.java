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

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Stream;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.operators.SimpleOperatorImpl;
import org.apache.samza.sql.operators.window.RetentionPolicy;
import org.apache.samza.sql.window.storage.*;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;

import java.util.List;

public abstract class WindowOp extends SimpleOperatorImpl {

  protected final WindowSpec spec;

  protected WindowStore windowStore;

  protected MessageStore messageStore;

  protected WindowOutputStream outputStream;

  protected boolean inputDisabled = false;

  public WindowOp(WindowSpec spec) {
    super(spec);
    this.spec = spec;
  }

  public WindowOp(WindowSpec spec, OperatorCallback callback) {
    super(spec, callback);
    this.spec = spec;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public void init(Config config, TaskContext context) throws Exception {
    this.windowStore = new WindowStore(
        (KeyValueStore<OrderedStoreKey, WindowState>) context.getStore("wnd-store-" + getId()));
    this.messageStore = (MessageStore) context.getStore("wnd-msg-" + getId());
    this.outputStream = new WindowOutputStream<OrderedStoreKey>((Stream<OrderedStoreKey>) context.getStore(spec.getOutputName().getName()),
        spec.getOutputName(), null);
    // TODO: fix message store spec
  }

  protected RetentionPolicy getRetentionPolicy() {
    return spec.getRetentionPolicy();
  }

  protected String getId() {
    return spec.getId();
  }

  public boolean isInputDisabled() {
    return inputDisabled;
  }

  public void setInputDisabled(boolean inputDisabled) {
    this.inputDisabled = inputDisabled;
  }

  protected Entry<OrderedStoreKey, WindowState> getFirstWindow() {
    KeyValueIterator<OrderedStoreKey, WindowState> iterator = this.windowStore.all();

    while (iterator.hasNext()) {
      Entry<OrderedStoreKey, WindowState> window = iterator.next();

      if (!isExpired(window.getValue())) {
        return window;
      }
    }

    return null;
  }

  protected abstract boolean isExpired(WindowState window);

  protected abstract boolean isExpired(Tuple tuple);

  protected abstract boolean isReplay(Tuple tuple);

  protected abstract List<OrderedStoreKey> getWindows(Tuple tuple);

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
}

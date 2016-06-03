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

package org.apache.samza.sql.window.storage;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.List;
import java.util.Map;
import java.util.Set;


public class WindowStore implements KeyValueStore<OrderedStoreKey, WindowState> {

  private final KeyValueStore<OrderedStoreKey, WindowState> underlying;

  public WindowStore(KeyValueStore<OrderedStoreKey, WindowState> underlying) {
    this.underlying = underlying;
  }

  @Override
  public WindowState get(OrderedStoreKey key) {
    return this.underlying.get(key);
  }

  @Override
  public Map<OrderedStoreKey, WindowState> getAll(List<OrderedStoreKey> keys) {
      return null;
  }

  @Override
  public void put(OrderedStoreKey key, WindowState value) {
    this.underlying.put(key, value);
  }

  @Override
  public void putAll(List<Entry<OrderedStoreKey, WindowState>> entries) {
    this.underlying.putAll(entries);
  }

  @Override
  public void delete(OrderedStoreKey key) {
    this.underlying.delete(key);
  }

  @Override
  public void deleteAll(List<OrderedStoreKey> keys) {

  }

  @Override
  public KeyValueIterator<OrderedStoreKey, WindowState> range(OrderedStoreKey from, OrderedStoreKey to) {
    return this.underlying.range(from, to);
  }

  @Override
  public KeyValueIterator<OrderedStoreKey, WindowState> all() {
    return this.underlying.all();
  }

  @Override
  public void close() {
    this.underlying.close();
  }

  @Override
  public void flush() {
    this.underlying.flush();
  }

  public void flush(Set<OrderedStoreKey> pendingFlushWindows) {
    //TODO: write the buffered window state updates to disk
  }

}

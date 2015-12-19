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

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Stream;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.sql.window.storage.TimeAndOffsetKey;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.List;

public class StoreStream implements Stream<OrderedStoreKey> {

  private final KeyValueStore<OrderedStoreKey, Tuple> underlying;

  public StoreStream(KeyValueStore<OrderedStoreKey, Tuple> underlying) {
    this.underlying = underlying;
  }

  @Override
  public List<String> getOrderFields() {
    return null;
  }

  @Override
  public EntityName getName() {
    return null;
  }

  @Override
  public Tuple get(OrderedStoreKey key) {
    return underlying.get(key);
  }

  @Override
  public void put(OrderedStoreKey key, Tuple value) {
    underlying.put(key, value);
  }

  @Override
  public void putAll(List<Entry<OrderedStoreKey, Tuple>> entries) {
  underlying.putAll(entries);
  }

  @Override
  public void delete(OrderedStoreKey key) {
  underlying.delete(key);
  }

  @Override
  public KeyValueIterator<OrderedStoreKey, Tuple> range(OrderedStoreKey from, OrderedStoreKey to) {
    return underlying.range(from, to);
  }

  @Override
  public KeyValueIterator<OrderedStoreKey, Tuple> all() {
    return underlying.all();
  }

  @Override
  public void close() {
    underlying.close();
  }

  @Override
  public void flush() {
    underlying.flush();
  }
}

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

import java.util.List;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Stream;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;


/**
 * This class defines an ordered and enumerable stream of window output values.
 *
 */
public class WindowOutputStream<K extends Comparable<?>> implements Stream<K> {
  /**
   * The underlying stream storage. For window output, it should be {@link org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStore}
   */
  private final Stream<K> underlying;

  /**
   * The corresponding stream {@link org.apache.samza.sql.api.data.EntityName} that uniquely identifies this window output stream
   */
  private final EntityName strmName;

  /**
   * The {@link org.apache.samza.sql.window.storage.MessageStoreSpec} that specify the message store key-value structure
   */
  private final MessageStoreSpec spec;

  public WindowOutputStream(Stream<K> store, EntityName outStrm, MessageStoreSpec spec) {
    this.underlying = store;
    this.strmName = outStrm;
    this.spec = spec;
  }

  @Override
  public EntityName getName() {
    return this.strmName;
  }

  @Override
  public Tuple get(K key) {
    return underlying.get(key);
  }

  @Override
  public void put(K key, Tuple value) {
    underlying.put(key, value);
  }

  @Override
  public void putAll(List<Entry<K, Tuple>> entries) {
    underlying.putAll(entries);
  }

  @Override
  public void delete(K key) {
    underlying.delete(key);
  }

  @Override
  public KeyValueIterator<K, Tuple> range(K from, K to) {
    return underlying.range(from, to);
  }

  @Override
  public KeyValueIterator<K, Tuple> all() {
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

  @Override
  public List<String> getOrderFields() {
    return this.spec.getOrderFields();
  }

  /**
   * A helper function to clear all entries in the stream
   */
  public void clear() {
    KeyValueIterator<K, Tuple> iter = this.all();
    while (iter.hasNext()) {
      underlying.delete(iter.next().getKey());
    }
  }

  public MessageStoreSpec getSpec() {
    return this.spec;
  }

}

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

import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;

import java.util.List;


/**
 * This class defines a {@link org.apache.samza.storage.kv.KeyValueIterator} that takes a list of filters and only returns entries matching the filters
 * when traverse through the store
 *
 * @param <K> The key type of the {@link org.apache.samza.storage.kv.KeyValueIterator}
 */
public class FilteredMessageIterator<K> implements KeyValueIterator<K, Tuple> {

  /**
   * Underlying {@link org.apache.samza.storage.kv.KeyValueIterator} iterator
   */
  private final KeyValueIterator<K, Tuple> underlying;

  /**
   * Last matched item
   */
  Entry<K, Tuple> lastItem = null;

  /**
   * List of entries of pairs: (field name, field value) that are used as filters
   */
  private final List<Entry<String, Object>> filters;

  public FilteredMessageIterator(KeyValueIterator<K, Tuple> underlying, List<Entry<String, Object>> filters) {
    this.underlying = underlying;
    this.filters = filters;
    hasNext();
  }

  @Override
  public boolean hasNext() {
    while (this.underlying.hasNext()) {
      Entry<K, Tuple> next = this.underlying.next();
      if (matchFilters(next)) {
        this.lastItem = next;
        return true;
      }
    }
    return false;
  }

  @Override
  public Entry<K, Tuple> next() {
    Entry<K, Tuple> retItem = this.lastItem;
    hasNext();
    return retItem;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Auto-filter iterator is read-only and does not support remove");
  }

  @Override
  public void close() {
    this.underlying.close();
  }

  private boolean matchFilters(Entry<K, Tuple> entry) {
    for(Entry<String, Object> fieldEntry : filters) {
      if (!fieldEntry.getValue().equals(entry.getValue().getMessage().getFieldData(fieldEntry.getKey()))) {
        return false;
      }
    }
    return true;
  }
}

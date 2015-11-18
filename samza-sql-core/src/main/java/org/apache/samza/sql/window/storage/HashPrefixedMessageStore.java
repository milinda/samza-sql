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

import java.util.ArrayList;
import java.util.List;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Stream;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;


/**
 * This class implements a {@link org.apache.samza.sql.window.storage.MessageStore} that uses field values as prefix
 */
public class HashPrefixedMessageStore extends MessageStore {

  private final char PREFIX_DENOMINATOR = '_';

  public HashPrefixedMessageStore(Stream<OrderedStoreKey> msgStore, EntityName strmName, MessageStoreSpec spec) {
    super(msgStore, strmName, spec);
  }

  private String getPrefix(List<Entry<String, Object>> filterFields) {
    StringBuffer strBuffer = new StringBuffer();
    List<String> prefixFields = this.getSpec().getPrefixFields();
    int fieldsFound = 0;
    for (Entry<String, Object> entry : filterFields) {
      strBuffer.append(entry.getValue().toString()).append(PREFIX_DENOMINATOR);
      if (prefixFields.contains(entry.getKey())) {
        fieldsFound++;
        // remove the field from the {@code filterFields} since it is already encoded in the prefix
        filterFields.remove(entry);
      }
    }
    if (fieldsFound < prefixFields.size()) {
      throw new IllegalArgumentException(
          "The filter fields in a range scan have to include all prefix fields in HashPrefixedMessageStore");
    }
    return strBuffer.toString();
  }

  private Range<OrderedStoreKey> getPrefixedRange(Range<OrderedStoreKey> range, List<Entry<String, Object>> filterFields) {
    String prefixStr = this.getPrefix(filterFields);
    return Range.<OrderedStoreKey>between(new PrefixedKey(prefixStr, range.getMin()), new PrefixedKey(prefixStr, range.getMax()));
  }

  @Override
  public OrderedStoreKey getKey(OrderedStoreKey extKey, Tuple tuple) {
    StringBuilder prefixStr = new StringBuilder();
    for (String field : this.getSpec().getPrefixFields()) {
      prefixStr.append(tuple.getMessage().getFieldData(field).toString()).append(PREFIX_DENOMINATOR);
    }
    return new PrefixedKey(prefixStr.toString(), extKey);
  }

  @Override
  public KeyValueIterator<OrderedStoreKey, Tuple> getMessages(Range<OrderedStoreKey> extRange,
      List<Entry<String, Object>> filterFields) {
    List<Entry<String, Object>> internalFilters = new ArrayList<Entry<String, Object>>();
    internalFilters.addAll(filterFields);
    Range<OrderedStoreKey> prefixRange = this.getPrefixedRange(extRange, internalFilters);
    // Now, the list of filters are reduced and part of the filter fields are in the prefixRange already to allow faster range scan
    return super.getMessages(prefixRange, internalFilters);
  }

  @Override
  public void purge(Range<OrderedStoreKey> extRange) {
    // Naive implementation of purge for now, will be costly since it traverses through all possible prefix keys
    KeyValueIterator<OrderedStoreKey, Tuple> iter = this.all();
    while (iter.hasNext()) {
      Entry<OrderedStoreKey, Tuple> entry = iter.next();
      PrefixedKey key = (PrefixedKey) entry.getKey();
      if (extRange.contains(key.getKey()) || extRange.getMin().compareTo(key.getKey()) > 0) {
        this.delete(entry.getKey());
      }
    }
  }
}

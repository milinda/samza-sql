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

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Stream;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;

import java.util.List;


/**
 * This class defines the default {@code MessageStore} for {@link org.apache.samza.sql.operators.window.WindowOp}.
 */
public class MessageStore extends WindowOutputStream<OrderedStoreKey> {

  /**
   * Ctor for {@code MessageStore}
   *
   * @param msgStore The underlying store used for messages. It can either be in-memory or on-disk, and should not be a logged store to avoid performance issue.
   * @param strmName The stream name that uniquely identifies the message stream
   * @param spec The specification of the message store
   */
  public MessageStore(Stream<OrderedStoreKey> msgStore, EntityName strmName, MessageStoreSpec spec) {
    super(msgStore, strmName, spec);
  }

  /**
   * Get the list of messages within the specified key range and apply the filters
   *
   * @param extRange The range of external keys to query from
   * @param filterFields The list of filter fields and values to match
   * @return The iterator for the list of matching messages.
   */
  public KeyValueIterator<OrderedStoreKey, Tuple> getMessages(Range<OrderedStoreKey> extRange,
                                                              List<Entry<String, Object>> filterFields) {
    // returns an iterator that automatically filters entries based on the filterFields
    if (filterFields == null || filterFields.isEmpty()) {
      return this.range(extRange.getMin(), extRange.getMax());
    }

    return new FilteredMessageIterator<OrderedStoreKey>(this.range(extRange.getMin(), extRange.getMax()),
        filterFields);
  }

  /**
   * Get the message store key for the corresponding tuple
   *
   * @param extKey The external key used to identify the tuple in the stream
   * @param tuple The incoming message tuple
   * @return The actual message store key to access the tuple in this {@code MessageStore}
   */
  public OrderedStoreKey getKey(OrderedStoreKey extKey, Tuple tuple) {
    return extKey;
  }

  /**
   * Get rid of a range of old messages
   *
   * @param extRange The external key range to be cleaned up
   */
  public void purge(Range<OrderedStoreKey> extRange) {
    KeyValueIterator<OrderedStoreKey, Tuple> iter = this.range(extRange.getMin(), extRange.getMax());
    while (iter.hasNext()) {
      this.delete(iter.next().getKey());
    }
  }
}

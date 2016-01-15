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

package org.apache.samza.sql.bench;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.bench.utils.DataVerifier;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.physical.window.TimeBasedSlidingWindowAggregatorState;
import org.apache.samza.sql.physical.window.codegen.WindowOperator;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.sql.window.storage.TimeAndOffsetKey;
import org.apache.samza.sql.window.storage.TimeKey;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.sql.LongOffset;
import org.apache.samza.task.*;

import java.util.HashMap;

public class SlidingWindowNativeStreamTask implements StreamTask, InitableTask {

  private static final String LOWER = "lower";
  private static final String UPPER = "upper";
  private static final Long INITIAL_LOWERBOUND = 9223372036854775807L;

  private KeyValueStore<TimeAndOffsetKey, GenericRecord> messageStore = null;
  private KeyValueStore<TimeKey, TimeBasedSlidingWindowAggregatorState> windowState = null;
  private KeyValueStore<String, Long> windowBounds = null;
  private KeyValueStore<Integer, Integer> aggregateState = null;
  private int windowSize = 1; // in minutes
  private Schema outputSchema;
  private final SystemStream outputStream = new SystemStream("kafka", "slidingnativeoutput");

  @Override
  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) throws Exception {
    this.messageStore = (KeyValueStore<TimeAndOffsetKey, GenericRecord>) context.getStore("messagestore");
    this.windowBounds = (KeyValueStore<String, Long>) context.getStore("windowbounds");
    this.windowState = (KeyValueStore<TimeKey, TimeBasedSlidingWindowAggregatorState>) context.getStore("windowstate");
    this.aggregateState = (KeyValueStore<Integer, Integer>) context.getStore("aggstate");
    this.outputSchema = new Schema.Parser().parse(DataVerifier.loadSchema(DataVerifier.SchemaType.SLIDINGWINDOW));
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    if (!(envelope.getMessage() instanceof GenericRecord)) {
      throw new SamzaException("Unsupported message type: " + envelope.getMessage().getClass());
    }

    LongOffset messageOffset = new LongOffset(envelope.getOffset());
    GenericRecord message = (GenericRecord) envelope.getMessage();

    Long rowTime = (Long) message.get("rowtime");

    if (isReplay(rowTime, messageOffset)) {
      return;
    }

    if(rowTime > getWindowUpperbound()) {
      updateUpperBound(rowTime);
    }

    if(getWindowLowerbound() == INITIAL_LOWERBOUND) {
      updateLowerBound(rowTime);
    } else {
      Long newLowerBound = rowTime - windowSize * 60000;
      if(newLowerBound > 0) {
        updateLowerBound(newLowerBound);
      }
    }

    // add current tuple to window
    TimeKey wsKey = new TimeKey(rowTime);
    TimeBasedSlidingWindowAggregatorState waState = windowState.get(wsKey);
    if(waState != null) {
      waState.addTuple(rowTime, messageOffset);
    } else {
      waState = new TimeBasedSlidingWindowAggregatorState(rowTime, messageOffset);
    }

    windowState.put(wsKey, waState);

    Integer units = (Integer) message.get("units");
    Integer productId = (Integer) message.get("productId");
    Integer orderId = (Integer) message.get("orderId");

    KeyValueIterator<TimeKey, TimeBasedSlidingWindowAggregatorState> messagesToPurge =
        windowState.range(new TimeKey(0L), new TimeKey(getWindowLowerbound()));

    while(messagesToPurge.hasNext()) {
      Entry<TimeKey, TimeBasedSlidingWindowAggregatorState> entry = messagesToPurge.next();
      for (TimeAndOffsetKey key : entry.getValue().getTuples()) {
        if (key != null) {
          GenericRecord rec = messageStore.get(key);
          Integer productIdToPurge = (Integer) rec.get("productId");
          Integer unitsToPurge = (Integer) rec.get("units");
          if(aggregateState.get(productIdToPurge) != null) {
            aggregateState.put(productIdToPurge, aggregateState.get(productIdToPurge) - unitsToPurge);
          }
        }
      }

      // Delete messages from current window
      windowState.delete(entry.getKey());
    }

    if(aggregateState.get(productId) != null) {
      aggregateState.put(productId, aggregateState.get(productId) + units);
    } else {
      aggregateState.put(productId, units);
    }


    GenericRecord out = new GenericRecordBuilder(outputSchema)
        .set("rowtime", rowTime)
        .set("productId", productId)
        .set("units", units)
        .set("unitsLastHour", aggregateState.get(productId))
        .build();

    collector.send(new OutgoingMessageEnvelope(outputStream, envelope.getKey(), out));

    messageStore.put(new TimeAndOffsetKey(rowTime, messageOffset), message);
  }

  private boolean isReplay(Long tupleTimestamp, LongOffset tupleOffset) {
    return messageStore.get(new TimeAndOffsetKey(tupleTimestamp, tupleOffset)) != null;
  }

  private Long getWindowUpperbound() {
    return windowBounds.get(UPPER) == null ? -1L : windowBounds.get(UPPER);
  }

  private Long getWindowLowerbound() {
    return windowBounds.get(LOWER) == null ? INITIAL_LOWERBOUND :  windowBounds.get(LOWER);
  }

  private void updateLowerBound(Long bound) {
    windowBounds.put(LOWER, bound);
  }

  private void updateUpperBound(Long bound) {
    windowBounds.put(UPPER, bound);
  }
}

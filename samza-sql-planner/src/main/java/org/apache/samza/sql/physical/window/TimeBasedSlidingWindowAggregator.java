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

package org.apache.samza.sql.physical.window;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.operators.SimpleOperatorImpl;
import org.apache.samza.sql.window.storage.MessageStore;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

import java.util.List;

public class TimeBasedSlidingWindowAggregator extends SimpleOperatorImpl {

  private final RelDataType type;

  private final TimeBasedSlidingWindowAggregatorSpec spec;

  /**
   * Keep messages we have seen during message retention period
   */
  private MessageStore messageStore;

  /**
   * Groups belonging to windows in the current retention period
   */
  private List<SlidingWindowGroup> groups;

  public TimeBasedSlidingWindowAggregator(TimeBasedSlidingWindowAggregatorSpec spec, RelDataType type) {
    super(spec);
    this.type = type;
    this.spec = spec;
  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void realProcess(Tuple ituple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // TODO: Create window store per group
//    this.windowStore = new WindowStore(
//        (KeyValueStore<OrderedStoreKey, TimeBasedSlidingWindowAggregatorState>) context.getStore("wnd-store-" + spec.getId()));
    this.messageStore = (MessageStore) context.getStore("wnd-msg-" + spec.getId());
  }

  private long getMessageTimeNano(Tuple tuple) {
//    String timestampField = spec.getTimestampField();
//    if (timestampField != null) {
//      return spec.getNanoTime(tuple.getMessage().getFieldData(timestampField).longValue());
//    }

    return tuple.getCreateTimeNano();
  }


}

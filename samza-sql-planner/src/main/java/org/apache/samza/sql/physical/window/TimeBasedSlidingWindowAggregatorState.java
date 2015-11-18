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

import org.apache.samza.sql.window.storage.TimeAndOffsetKey;
import org.apache.samza.system.sql.Offset;

import java.util.ArrayList;
import java.util.List;

/**
 * We have to use this to keep the sate because there can be multiple messages with same timestamp
 */
public class TimeBasedSlidingWindowAggregatorState {
  /**
   * Multiple tuples can have same timestamp.
   */
  private List<TimeAndOffsetKey> tuples = new ArrayList<TimeAndOffsetKey>();

  public TimeBasedSlidingWindowAggregatorState(Long tupleTimestamp, Offset tupleOffset) {
    tuples.add(new TimeAndOffsetKey(tupleTimestamp, tupleOffset));
  }

  public void addTuple(long timestamp, Offset offset) {
    tuples.add(new TimeAndOffsetKey(timestamp, offset));
  }

  public List<TimeAndOffsetKey> getTuples() {
    return tuples;
  }
}

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

import org.apache.calcite.util.Pair;

import java.util.*;

/**
 * Implements simple time/tuple based sliding window. This will be used as a template for actual SQL operator
 * implementation.
 * <p/>
 * TODO
 * ----
 * - How to store window?
 */
public class SlidingWindowTemplate {

  public static final Long MINUTE_TO_NANO_MULTIPLIER = 60000000000L;
  public static final Long MILLI_TO_NANO_MULTIPLIER = 1000000L;

  public enum Aggregate {
    SUM,
    COUNT
  }

  /**
   * how many nano seconds to look back when computing sliding window
   */
  private final long precedingNanos;

  private final int timestampFieldIdx;

  private final int operandFieldIdx;

  private Long latestTimeStamp = -1L;

  private Long windowLowerBound = Long.MAX_VALUE;

  private final Aggregate aggregate;

  private final Map<Long, List<Object[]>> window = new HashMap<Long, List<Object[]>>();

  private Long accumulator = 0L;

  private static final Random rand = new Random(System.currentTimeMillis());

  public SlidingWindowTemplate(long precedingNanos, int timestampFieldIdx, int operandFieldIdx, Aggregate aggregate) {
    this.precedingNanos = precedingNanos;
    this.timestampFieldIdx = timestampFieldIdx;
    this.operandFieldIdx = operandFieldIdx;
    this.aggregate = aggregate;
  }

  public Object[] send(Pair<Integer, Object[]> tupleWithOffset) {
    // Assumes we get timestamp in nano seconds
    Long tupleTimeStamp = (Long) tupleWithOffset.getValue()[timestampFieldIdx];

    if (tupleTimeStamp > latestTimeStamp) {
      // Here we don't consider system time for calculation of window boundary. If we receive a future event we will
      // have to adjust the time according to that. This can be a problem when window size is small and there is
      // considerable amount of out-of-order messages
      latestTimeStamp = tupleTimeStamp;
    }

    // Below logic updates the window only when there is a change to window lower bound. For this to happen, current
    // tuple's timestamp should be greater than max time stamp we have seen up to now
    if(latestTimeStamp > precedingNanos) {
      windowLowerBound = latestTimeStamp - precedingNanos;
    } else {
      if (tupleTimeStamp < windowLowerBound) {
        windowLowerBound = tupleTimeStamp;
      }
    }

    System.out.println("Lower bound: " + windowLowerBound);

    List<Long> tuplesToRemove = new ArrayList<Long>();
    for (Map.Entry<Long, List<Object[]>> message : window.entrySet()) {
      if (message.getKey() < windowLowerBound) {
        for (Object[] m : message.getValue()) {
          if (m[operandFieldIdx] != null) {
            if (aggregate == Aggregate.SUM) {
              accumulator -= (Long) m[operandFieldIdx];
            } else if (aggregate == Aggregate.COUNT) {
              accumulator -= 1L;
            }
          }
        }

        tuplesToRemove.add(message.getKey());
        System.out.println(String.format("Removed message with timestamp %s from window.", String.valueOf(message.getKey())));
      }
    }

    for(Long r : tuplesToRemove) {
      window.remove(r);
    }

    if (tupleTimeStamp >= windowLowerBound) {
      if (!window.containsKey(tupleTimeStamp)) {
        ArrayList<Object[]> messages = new ArrayList<Object[]>();
        window.put(tupleTimeStamp, messages);
      }

      window.get(tupleTimeStamp).add(tupleWithOffset.getValue());

      if (tupleWithOffset.getValue()[operandFieldIdx] != null) {
        if (aggregate == Aggregate.SUM) {
          accumulator += (Long) tupleWithOffset.getValue()[operandFieldIdx];
        } else if (aggregate == Aggregate.COUNT) {
          accumulator += 1L;
        }
      }

      Object[] output = new Object[tupleWithOffset.getValue().length + 1];

      for (int i = 0; i < tupleWithOffset.getValue().length; i++) {
        output[i] = tupleWithOffset.getValue()[i];
      }

      output[output.length - 1] = accumulator;

      return output;
    }

    // If tuple's timestamp is less than window's lower bound, do we need to emit any values other than updating the
    // window

    return null;
  }

  public static void main(String[] args) {
    SlidingWindowTemplate sw = new SlidingWindowTemplate(2, 0, 1, Aggregate.SUM);

    long currentMills = 1L;
    Object[][] inputs = {
        {1L, 3L},
        {2L, 4L},
        {3L, 5L},
        {2L, 3L},
        {1L, 2L},
        {4L, 2L},
        {5L, 1L}};

    int i = 0;
    for (Object[] msg : inputs) {
      System.out.println(String.format("INPUT:\t %s\t%s", String.valueOf((Long) msg[0]), String.valueOf((Long) msg[1])));
      Object[] out = sw.send(Pair.of(i, msg));
      if (out != null) {
        System.out.println(String.format("OUTPUT:\t %s\t%s\t%s", String.valueOf((Long) out[0]), String.valueOf((Long) out[1]), String.valueOf((Long) out[2])));
      }
      i++;
    }

    Object[][] outputs = {
        {1L, 3L, 3L},
        {2L, 4L, 7L},
        {3L, 5L, 12L},
        {2L, 3L, 15L},
        {1L, 2L, 17L},
        {4L, 2L, 16L},
        {5L, 1L, 12L}};
  }
  // {1}, {1,2}, {1,2,3}, {1,2,3,2}, {1,2,3,2,1}, {2,3,2,4}
}

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

import org.apache.samza.system.sql.Offset;


/**
 * This class defines the window state data to be stored in {@link org.apache.samza.sql.operators.window.WindowOp} for each window.
 */
public class WindowState {
  /**
   * The start offset of the window (inclusive)
   */
  private final Offset startOffset;

  /**
   * The start time of the window in nano seconds (inclusive)
   */
  private final long stTimeNano;

  /**
   * The last offset of the message received in the window (inclusive)
   */
  private Offset lastOffset = null;

  /**
   * The end time of the window in nano seconds (exclusive)
   */
  private long edTimeNano = Long.MIN_VALUE;

  /**
   * The optional per-window aggregate result
   */
  private Object value = null;

  public WindowState(Offset startOffset, long startTimeNano, long endTimeNano) {
    this.startOffset = startOffset;
    this.lastOffset = null;
    this.stTimeNano = startTimeNano;
    this.edTimeNano = endTimeNano;
    this.value = null;
  }

  /**
   * Set the last offset in this window.
   *
   * <p>NOOP if {@code offset} &lt;= this window's {@code lastOffset}
   *
   * @param offset The offset to be set
   */
  public void setLastOffset(Offset offset) {
    if (offset.compareTo(lastOffset) <= 0) {
      // NOOP
      return;
    }
    this.lastOffset = offset;
  }

  /**
   * Set the end time of this window in nano seconds.
   *
   * <p> NOOP if {@code timeNano} &lt;= this window's current end time in nanosecond
   *
   * @param timeNano The end time to be set
   */
  public void setEndTime(long timeNano) {
    if (timeNano <= this.edTimeNano) {
      // NOOP
      return;
    }
    this.edTimeNano = timeNano;
  }

  /**
   * Set the aggregated value corresponding to this window
   *
   * @param value The aggregated value to be set
   */
  public void setValue(Object value) {
    this.value = value;
  }

  /**
   * Get this window's {@code startOffset}
   *
   * @return The {@code startOffset} of this window
   */
  public Offset getStartOffset() {
    return this.startOffset;
  }

  /**
   * Get this window's {@code lastOffset}
   *
   * @return This window's {@code lastOffset}
   */
  public Offset getLastOffset() {
    return this.lastOffset;
  }

  /**
   * Get this window's start time in nanosecond
   *
   * @return This window's start time in nanosecond
   */
  public long getStartTimeNano() {
    return this.stTimeNano;
  }

  /**
   * Get this window's end time in nanosecond
   *
   * @return This window's end time in nanosecond
   */
  public long getEndTimeNano() {
    return this.edTimeNano;
  }

  /**
   * Get the window's aggregated value
   *
   * @return This window's aggregated value
   */
  public Object getValue() {
    return this.value;
  }
}

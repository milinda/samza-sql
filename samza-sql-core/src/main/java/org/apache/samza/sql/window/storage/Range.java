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

/**
 * This class implements a half-open range class: [start, end)
 */
public class Range<T extends Comparable<T>> {
  private final T minValue;
  private final T maxValue;

  private Range(T t1, T t2) {
    this.minValue = t1;
    this.maxValue = t2;
  }

  public static <T extends Comparable<T>> Range<T> between(T t1, T t2) {
    return new Range<T>(t1, t2);
  }

  public boolean contains(T t1) {
    return this.minValue.compareTo(t1) <= 0 && this.maxValue.compareTo(t1) > 0;
  }

  public T getMin() {
    return minValue;
  }

  public T getMax() {
    return maxValue;
  }
}

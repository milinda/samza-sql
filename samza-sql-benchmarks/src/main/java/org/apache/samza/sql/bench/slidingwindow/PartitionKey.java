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

package org.apache.samza.sql.bench.slidingwindow;

import java.util.Arrays;

public class PartitionKey {
  private final Object[] values;

  public PartitionKey(Object[] values) {
    this.values = values;
  }

  public static PartitionKey of(Object o1) {
    return new PartitionKey(new Object[]{o1});
  }

  public static PartitionKey of(Object o1, Object o2) {
    return new PartitionKey(new Object[]{o1, o2});
  }

  public static PartitionKey of(Object o1, Object o2, Object o3) {
    return new PartitionKey(new Object[]{o1, o2, o3});
  }

  public static PartitionKey of(Object... values) {
    return new PartitionKey(values);
  }

  public Object[] getValues() {
    return values;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || obj instanceof PartitionKey && Arrays.equals(values, ((PartitionKey) obj).values);
  }

  @Override
  public String toString() {
    return Arrays.toString(values);
  }

  public static class Builder {
    Object[] values;

    public Builder(int size) {
      values = new Object[size];
    }

    public void set(Integer index, Object value) {
      values[index] = value;
    }

    public PartitionKey build() {
      return new PartitionKey(values);
    }

    public int size() {
      return values.length;
    }
  }
}


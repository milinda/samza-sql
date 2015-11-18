/**
 * Copyright (C) 2015 Trustees of Indiana University
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.samza.sql.operators.aggregate;

import java.util.Arrays;

public class GroupKey {
  private final Object[] values;

  private GroupKey(Object[] values) {
    this.values = values;
  }

  public static GroupKey of(Object... values) {
    return new GroupKey(values);
  }

  public static GroupKey of(Object value0) {
    return new GroupKey(new Object[]{value0});
  }

  public static GroupKey of(Object value0, Object value1) {
    return new GroupKey(new Object[]{value0, value1});
  }

  public static GroupKey of(Object value0, Object value1, Object value2) {
    return new GroupKey(new Object[]{value0, value1, value2});
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || obj instanceof GroupKey && Arrays.equals(values, ((GroupKey) obj).values);
  }

  public Object get(int index) {
    return values[index];
  }

  public Object[] getValues() {
    return values;
  }

  public int size() {
    return values.length;
  }
}

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
 * This class defines keys that are based on {@link org.apache.samza.system.sql.Offset}
 */
public class OffsetKey extends OrderedStoreKey {
  private final Offset offset;

  public OffsetKey(Offset offset) {
    this.offset = offset;
  }

  @Override
  public int compareTo(OrderedStoreKey o) {
    if (!(o instanceof OffsetKey)) {
      throw new IllegalArgumentException("Cannot compare OffsetMessageKey with other type of keys. Other key type:"
          + o.getClass().getName());
    }
    OffsetKey other = (OffsetKey) o;
    return this.offset.compareTo(other.offset);
  }

}

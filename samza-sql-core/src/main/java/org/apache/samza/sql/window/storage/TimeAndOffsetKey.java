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
 * This class implements key that is composed of: (time, offset)
 */
public class TimeAndOffsetKey extends OrderedStoreKey {
  private final Offset offset;
  private final Long timeNano;

  public TimeAndOffsetKey(long timeNano, Offset offset) {
    this.timeNano = timeNano;
    this.offset = offset;
  }

  @Override
  public int compareTo(OrderedStoreKey o) {
    if (!(o instanceof TimeAndOffsetKey)) {
      throw new IllegalArgumentException("Cannot compare TimeAndOffsetKey with other type of keys. Other key type:"
          + o.getClass().getName());
    }
    TimeAndOffsetKey other = (TimeAndOffsetKey) o;
    if (this.timeNano.compareTo(other.timeNano) != 0) {
      return this.timeNano.compareTo(other.timeNano);
    }
    return this.offset.compareTo(other.offset);
  }

}

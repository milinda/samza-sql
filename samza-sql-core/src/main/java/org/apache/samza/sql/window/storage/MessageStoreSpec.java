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

import java.util.ArrayList;
import java.util.List;


public class MessageStoreSpec {
  public enum StoreType {
    PREFIX_STORE,
    OFFSET_STORE,
    TIME_AND_OFFSET_STORE
  }

  private final StoreType type;
  private final List<String> prefixFields;
  private final String timestampField;
  private final List<String> fullOrderFields = new ArrayList<String>();

  public MessageStoreSpec(StoreType type, List<String> prefixFields, String timestampField) {
    this.type = type;
    this.prefixFields = prefixFields;
    this.timestampField = timestampField;
    if (prefixFields != null && !prefixFields.isEmpty()) {
      this.fullOrderFields.addAll(prefixFields);
    }
    if (timestampField != null && !timestampField.isEmpty()) {
      this.fullOrderFields.add(timestampField);
    }

  }

  public List<String> getOrderFields() {
    return this.fullOrderFields;
  }

  public List<String> getPrefixFields() {
    return this.prefixFields;
  }

  public String getTimestampField() {
    return this.timestampField;
  }

  public StoreType getType() {
    return this.type;
  }
}

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

package org.apache.samza.sql.jdbc.utils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.*;

import java.util.Map;

/**
 * TableFactory implementation that works on custom table spec with Avro schema and other parameters
 * as operands.
 *
 * Example table description:
 * {
 *   type : 'custom',
 *   name : 'Orders',
 *   stream : {
 *     stream : true
 *   },
 *   factory : 'org.apache.samza.sql.jdbc.utils.AvroTableFactory',
 *   operand : {
 *     schema : 'avro-schema goes here',
 *     timestamp : 'timestamp-field',
 *     pkey : 'primary-key'
 *   }
 * }
 */
public class AvroTableFactory implements TableFactory<Table> {
  @Override
  public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {

    return null;
  }
}

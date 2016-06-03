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

package org.apache.samza.sql.calcite.schema;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.*;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.samza.SamzaException;

import java.util.Map;

/**
 * Sample table definition in a Calcite model looks like following:
 * <p/>
 * <pre>
 * {
 *   name: 'Orders',
 *   type: 'custom',
 *   facotry: 'org.apache.samza.sql.calcite.schema.SamzaSQLTableFactory',
 *   operand: {
 *     stream : true,
 *     system : 'kafka',
 *     messageschematype: 'avro',
 *     messageschema: {
 *       "type": "records",
 *       "namespace": "com.example",
 *       "name": "FullName",
 *       "fields": [
 *         { "name": "first", "type": "string" },
 *         { "name": "last", "type": "string" }
 *       ]
 *     }
 *   }
 * }
 * </pre>
 */
public class SamzaSQLTable implements ScannableTable, StreamableTable, SamzaSQLStream, SamzaSQLExternalTable {

  private final boolean isStream;

  private final String schema;

  private final String name;

  private String messageSchema;

  private MessageSchemaType messageSchemaType;

  public SamzaSQLTable(String name, String schemaName, Map<String, Object> operands) {
    this.name = name;
    this.schema = schemaName;
    this.isStream = (Boolean) operands.get("stream") != null ? (Boolean) operands.get("stream") : true;

    if (operands.containsKey("messageschematype")) {
      this.messageSchemaType = MessageSchemaType.valueOf((String) operands.get("messageschematype"));
    } else {
      this.messageSchemaType = MessageSchemaType.AVRO;
    }

    if (operands.containsKey("messageschema")) {
      Map schema = (Map) operands.get("messageschema");
      Gson gson = new Gson();
      this.messageSchema = gson.toJson(schema);
    } else {
      throw new SamzaException("Cannot find required field 'messageschema'.");
    }
  }

  public SamzaSQLTable(boolean isStream, String schema, String name, String messageSchema, MessageSchemaType messageSchemaType) {
    this.isStream = isStream;
    this.schema = schema;
    this.name = name;
    this.messageSchema = messageSchema;
    this.messageSchemaType = messageSchemaType;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if(messageSchemaType != MessageSchemaType.AVRO) {
      return null;
    }
    return new AvroSchemaConverter(typeFactory, new org.apache.avro.Schema.Parser().parse(messageSchema)).convert();
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.of(100d,
        ImmutableList.<ImmutableBitSet>of(),
        RelCollations.createSingleton(1));
  }

  @Override
  public Schema.TableType getJdbcTableType() {
   // return isStream ? Schema.TableType.STREAM : Schema.TableType.TABLE;
    return Schema.TableType.STREAM;
  }

  public String getStreamName() {
    return name;
  }

  @Override
  public String getSystem() {
    return schema;
  }


  public String getMessageSchema() {
    return messageSchema;
  }

  public MessageSchemaType getMessageSchemaType() {
    return messageSchemaType;
  }

  @Override
  public Table stream() {
    return new SamzaSQLTable(isStream, schema, name, messageSchema, messageSchemaType);
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    return null;
  }
}

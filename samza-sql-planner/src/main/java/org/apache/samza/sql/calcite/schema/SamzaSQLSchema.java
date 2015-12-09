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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

/**
 * Schema mapped to a custom Calcite model JSON which supports Kafka Schema Registry as well as
 * other external data sources. Calcite JSON model with SamzaSQLSchema will look like following:
 *
 * <pre>
 * {
 *   version: '1.0',
 *   defaultSchema: 'SALES',
 *   schemas: [
 *     {
 *       name: 'SALES',
 *       type: 'custom',
 *       factory: 'org.apache.samza.sql.calcite.schema.SamzaSQLSchemaFactory',
 *       operand: {
 *         schemaregistry: 'http://localhost:8081',
 *         kafkabrokers: 'localhost:9092',
 *         zookeeper: 'localhost:2181'
 *       },
 *       tables: [
 *       {
 *         name: 'Orders',
 *         type: 'custom',
 *         facotry: 'org.apache.samza.sql.calcite.schema.SamzaSQLTableFactory',
 *         operand: {
 *           avroschema: {
 *             "type": "records",
 *             "namespace": "com.example",
 *             "name": "FullName",
 *             "fields": [
 *               { "name": "first", "type": "string" },
 *               { "name": "last", "type": "string" }
 *             ]
 *           }
 *         }
 *       },
 *       {
 *         name: 'FEMALE_EMPS',
 *         type: 'view',
 *         sql: 'SELECT * FROM emps WHERE gender = \'F\''
 *       }]
 *    }]
 * }
 * </pre>
 */
public class SamzaSQLSchema extends AbstractSchema {

  private final String schemaRegistryUrl;
  private final String brokersList;
  private final String zkConnectionString;

  public SamzaSQLSchema(String schemaRegistryUrl, String brokersList, String zkConnecitonString) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.brokersList = brokersList;
    this.zkConnectionString = zkConnecitonString;
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  public String getBrokersList() {
    return brokersList;
  }

  public String getZkConnectionString() {
    return zkConnectionString;
  }

  /**
   * Returns a map of tables representing topics registered in Kafka Schema Registry.
   *
   * @return table name to table map
   */
  @Override
  protected Map<String, Table> getTableMap() {
    // TODO: Read schema registry to create the table map.
    return super.getTableMap();
  }
}

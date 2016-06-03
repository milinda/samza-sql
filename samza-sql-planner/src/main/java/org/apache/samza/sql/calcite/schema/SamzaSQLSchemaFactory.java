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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory that creates a {@link SamzaSQLSchema}
 */
public class SamzaSQLSchemaFactory implements SchemaFactory {
  private static final Logger log = LoggerFactory.getLogger(SamzaSQLSchemaFactory.class);

  public static final String OPERAND_SCHEMA_REGISTRY = "schemaregistry";
  public static final String OPERAND_KAFKA_BROKERS = "kafkabrokers";
  public static final String OPERAND_ZK_CONNECTION_STRING = "zookeeper";

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    String schemaRegistryUrl;
    String kafkaBorkersList;
    String zkConnectionString;

    if(operand.containsKey(OPERAND_SCHEMA_REGISTRY)) {
      schemaRegistryUrl = (String)operand.get(OPERAND_SCHEMA_REGISTRY);
    } else {
      schemaRegistryUrl = null;
    }

    if (operand.containsKey(OPERAND_KAFKA_BROKERS)) {
      kafkaBorkersList = (String)operand.get(OPERAND_KAFKA_BROKERS);
    } else {
      log.error("Invalid schema. Missing operand " + OPERAND_KAFKA_BROKERS);
      return null;
    }

    if(operand.containsKey(OPERAND_ZK_CONNECTION_STRING)) {
      zkConnectionString = (String)operand.get(OPERAND_ZK_CONNECTION_STRING);
    } else {
      log.error("Invalid schema. Missing operand " + OPERAND_ZK_CONNECTION_STRING);
      return null;
    }

    return new SamzaSQLSchema(schemaRegistryUrl, kafkaBorkersList, zkConnectionString);
  }
}

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
package org.apache.samza.sql.planner.physical;

import org.apache.samza.sql.physical.JobConfigGenerator;
import org.apache.samza.sql.physical.JobConfigurations;
import org.apache.samza.sql.schema.SamzaSQLStream;
import org.apache.samza.sql.schema.SamzaSQLTable;

public class SamzaRelUtils {
  public static String deriveAndPopulateKeySerde(JobConfigGenerator configGenerator, SamzaSQLStream table) {
    String keySerdeName = String.format("%s-keyserde", table.getStreamName());
    SamzaSQLStream.KeyType keyType = table.getKeyType();
    if (keyType == SamzaSQLTable.KeyType.AVRO) {
      configGenerator.addSerde(keySerdeName, JobConfigGenerator.AVRO_SERDE_FACTORY);
      configGenerator.addConfig(
          String.format(JobConfigurations.SERIALIZERS_SCHEMA, keySerdeName),
          configGenerator.getQueryMetaStore().registerMessageType(configGenerator.getQueryId(),
              table.getKeySchema()));
    } else if (keyType == SamzaSQLStream.KeyType.INT) {
      configGenerator.addSerde(keySerdeName, JobConfigGenerator.INT_SERDE_FACTORY);
    } else if (keyType == SamzaSQLStream.KeyType.STRING) {
      configGenerator.addSerde(keySerdeName, JobConfigGenerator.STRING_SERDE_FACTORY);
    } else if (keyType == SamzaSQLStream.KeyType.LONG) {
      configGenerator.addSerde(keySerdeName, JobConfigGenerator.LONG_SERDE_FACTORY);
    } else {
      keySerdeName = null;
    }

    return keySerdeName;
  }
}

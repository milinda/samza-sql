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
package org.apache.samza.sql.api.metastore;

import java.io.File;

public interface SamzaSQLMetaStore {

  /**
   * Persist streaming SQL query to metadata store.
   *
   * @param query streaming SQL query
   * @return query id
   */
  String registerQuery(String query);

  /**
   * Reads query into a string
   *
   * @param queryId query id
   * @return query string
   */
  String getQuery(String queryId);

  /**
   * Persist message type to metadata store.
   *
   * @param queryId       query id
   * @param messageSchema string representation of message type (e.g. Avro schema)
   * @return message type id
   */
  String registerMessageType(String queryId, String messageSchema);

  /**
   * Read message type into a string
   *
   * @param queryId       query id
   * @param messageTypeId message type id
   * @return message type description
   */
  String getMessageType(String queryId, String messageTypeId);

  /**
   * Persist Calcite model to metadata store.
   *
   * @param queryId      query id
   * @param modelContent model content
   * @return model id
   */
  String registerCalciteModel(String queryId, String modelContent);

  /**
   * Persist Calcite mode file to metadata store.
   *
   * @param queryId   query id
   * @param modelFile model file
   * @return model id
   */
  String registerCalciteModel(String queryId, File modelFile);

  /**
   * Reads model into a string
   *
   * @param queryId query id
   * @param modelId model id
   * @return model
   */
  String getCalciteModel(String queryId, String modelId);

  void close();
}

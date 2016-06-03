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

package org.apache.samza.sql;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.samza.sql.api.Closeable;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Zookeeper backed query metadata store. Query metadata such as stream schemas and query it self
 * can be stored in Zookeeper via {@code QueryMetadataStore}.
 */
public class QueryMetadataStore implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(QueryMetadataStore.class);

  public static final String BASE_PATH = "/samzasql/%s";
  private CuratorFramework curatorFramework;

  public QueryMetadataStore(String zkConnectionString) {
    curatorFramework = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(1000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(zkConnectionString)
        .build();
    curatorFramework.start();
  }

  /**
   * Register new streaming sql query.
   * @param sql streaming sql query
   * @return query identifier
   */
  public String registerQuery(String sql) throws Exception {
    String queryPath = curatorFramework
        .create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
        .forPath(String.format(BASE_PATH, "query"), sql.getBytes());
    return queryPath.substring("/samzasql/".length());
  }

  /**
   * Get the streaming sql query identified by the query id.
   * @param queryId query identifier
   * @return streaming sql query
   */
  public String getQuery(String queryId) throws Exception {
    return new String(curatorFramework.getData().forPath(String.format(BASE_PATH, queryId)));
  }

  /**
   * Register schema's (only supports Avro) related streams and relations.
   *
   * @param queryId query identifier
   * @param schema string representation of the schema
   * @return schema identifier
   */
  public String registerSchema(String queryId, String schema) throws Exception {
    String schemaPath = curatorFramework
        .create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
        .forPath(String.format(BASE_PATH, queryId) + "/schema", schema.getBytes());
    return schemaPath.substring((String.format(BASE_PATH, queryId) + "/").length());
  }

  public String getSchema(String queryId, String schemaId) throws Exception  {
    return new String(curatorFramework
        .getData()
        .forPath(String.format(BASE_PATH, queryId) + "/" + schemaId));
  }


  public void close() {
    curatorFramework.close();
  }
}

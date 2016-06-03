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
package org.apache.samza.sql.metastore;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.api.metastore.SamzaSQLMetaStore;
import org.apache.zookeeper.CreateMode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ZKBackedQueryMetaStore implements SamzaSQLMetaStore {
  private static final String BASE_PATH = "/samzasql/%s";

  private CuratorFramework curatorFramework;

  public ZKBackedQueryMetaStore(String zkConnectionStr) {
    curatorFramework = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(1000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(zkConnectionStr)
        .build();
    curatorFramework.start();
  }

  @Override
  public String registerQuery(String query) throws SamzaException {
    try {
      String queryPath = curatorFramework
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
          .forPath(String.format(BASE_PATH, "query"), query.getBytes());
      return queryPath.substring("/samzasql/".length());
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public String getQuery(String queryId) {
    try {
      return new String(curatorFramework.getData().forPath(String.format(BASE_PATH, queryId)));
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public String registerMessageType(String queryId, String messageSchema) {
    try {
      String schemaPath = curatorFramework
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
          .forPath(String.format(BASE_PATH, queryId + "/schema"), messageSchema.getBytes());
      return schemaPath.substring(String.format(BASE_PATH, queryId + "/").length());
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public String getMessageType(String queryId, String messageTypeId) {
    try {
      return new String(curatorFramework
          .getData()
          .forPath(String.format(BASE_PATH, queryId + "/" + messageTypeId)));
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public String registerCalciteModel(String queryId, String modelContent) {
    try {
      String modelPath = curatorFramework
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
          .forPath(String.format(BASE_PATH, queryId + "/model"), modelContent.getBytes());
      return modelPath.substring(String.format(BASE_PATH, queryId + "/").length());
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public String registerCalciteModel(String queryId, File modelFile) {
    try {
      return registerCalciteModel(queryId, new String(Files.readAllBytes(Paths.get(modelFile.toURI()))));
    } catch (IOException e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public String getCalciteModel(String queryId, String modelId) {
    try {
      return new String(curatorFramework
          .getData()
          .forPath(String.format(BASE_PATH, queryId + "/" + modelId)));
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public void close() {
    if (curatorFramework != null && curatorFramework.getState() == CuratorFrameworkState.STARTED) {
      curatorFramework.close();
    }
  }
}

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

import junit.framework.Assert;
import org.apache.curator.test.TestingServer;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQueryMetadataStore {

  static  TestingServer zkServer;

  static String SELECT_QUERY = "select stream * from orders";

  static String SIMPLE_SCHEMA = "{\"type\": \"Record\", \"namespace\": \"org.apache.samza.sql\"}";

  @BeforeClass
  public static void setupZookeeper() throws Exception {
    zkServer = new TestingServer(true);
  }

  @Test
  public void testMetaStore() throws Exception {
    QueryMetadataStore metadataStore = new QueryMetadataStore(zkServer.getConnectString());
    String queryId = metadataStore.registerQuery(SELECT_QUERY);
    Assert.assertTrue(queryId.startsWith("query"));

    String query = metadataStore.getQuery(queryId);
    Assert.assertEquals(query, SELECT_QUERY);

    String schemaId = metadataStore.registerSchema(queryId, SIMPLE_SCHEMA);
    Assert.assertTrue(schemaId.startsWith("schema"));

    String schema = metadataStore.getSchema(queryId, schemaId);
    Assert.assertEquals(schema, SIMPLE_SCHEMA);

    metadataStore.close();
  }
}

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

package org.apache.samza.sql.master;

import org.apache.samza.sql.master.rest.entities.QuerySubmissionResponse;
import org.eclipse.jetty.server.Server;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;

public class SamzaSQLMasterTest {

  @BeforeClass
  public static void startSamzaSQLMaster() throws Exception {
    // Set system props for tests
    System.setProperty(SamzaSQLMaster.PROP_MODE, "dev");
    System.setProperty(SamzaSQLMaster.PROP_WEBAPP_BASE_DIR, "src/main/web");

    Server jettyServer = SamzaSQLMaster.createJettyServer();
    jettyServer.start();
  }

  @Test
  public void testQuerySubmission() {
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target("http://localhost:8080").path("api/query");

    String request = "{ \"query\" : \"select stream * from orders\" }";

    QuerySubmissionResponse response = target.request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE), QuerySubmissionResponse.class);
    Assert.assertEquals(response.getId(), 1L);
  }
}

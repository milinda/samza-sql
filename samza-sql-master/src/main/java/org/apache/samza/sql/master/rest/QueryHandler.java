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

package org.apache.samza.sql.master.rest;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.EbeanServer;
import org.apache.samza.sql.master.QueryManager;
import org.apache.samza.sql.master.model.Query;
import org.apache.samza.sql.master.rest.entities.QuerySubmissionRequest;
import org.apache.samza.sql.master.rest.entities.QuerySubmissionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

@Path("/query")
public class QueryHandler {
  private static final Logger log = LoggerFactory.getLogger(QueryHandler.class);

  private static final EbeanServer ebeanServer = Ebean.getServer("h2");

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public QuerySubmissionResponse submit(QuerySubmissionRequest request) {
    Query query = new Query();
    query.setQuery(request.getQuery());
    query.setStatus(Query.QueryStatus.ACCEPTED);
    ebeanServer.save(query);

    // Send the query for execution
    QueryManager.INSTANCE.executeQuery(new QueryManager.ExecQuery(query));

    return new QuerySubmissionResponse(query.getId());
  }
}

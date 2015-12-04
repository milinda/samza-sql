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

package org.apache.samza.sql.master.model;

import org.apache.samza.sql.master.model.finder.QueryFinder;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "ssql_query")
public class Query extends BaseModel {

  public static final QueryFinder find = new QueryFinder();

  @Column(length = 2048)
  String query;

  QueryStatus status;

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public QueryStatus getStatus() {
    return status;
  }

  public void setStatus(QueryStatus status) {
    this.status = status;
  }

  public static enum QueryStatus {
    ACCEPTED,
    RUNNING,
    FAILED,
    COMPLETED
  }
}

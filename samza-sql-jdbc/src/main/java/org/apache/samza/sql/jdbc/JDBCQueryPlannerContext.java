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

package org.apache.samza.sql.jdbc;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.samza.sql.planner.AbstractQueryPlannerContext;
import org.apache.samza.sql.schema.CalciteModelProcessor;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCQueryPlannerContext extends AbstractQueryPlannerContext {
  public static final String SCHEMA_URL = "schemaUrl";

  private final Properties properties;

  private final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

  private SchemaPlus defaultSchema;

  public JDBCQueryPlannerContext(Properties properties) throws URISyntaxException, IOException, SQLException {
    this.properties = properties;
    String schemaLocation =  properties.getProperty(SCHEMA_URL);
    URI schemaUri = new URI(schemaLocation);

    // If schemaUri is a URL we assume it points to a schema registry instance
    // If this a Calcite model based schema, jobs only works when client and yarn cluster is in
    // the same machine.
    // We can generate a JSON model and serve it via HTTP to Samza job
    boolean isUrl = "http".equalsIgnoreCase(schemaUri.getScheme()) ||
        "https".equalsIgnoreCase(schemaUri.getScheme());

    if(isUrl) {
      throw new RuntimeException("Doesn't support the schema registry yet.");
    }

    defaultSchema = new CalciteModelProcessor(schemaLocation, rootSchema).getDefaultSchema();
  }

  @Override
  public SchemaPlus getDefaultSchema() {
    return defaultSchema;
  }
}

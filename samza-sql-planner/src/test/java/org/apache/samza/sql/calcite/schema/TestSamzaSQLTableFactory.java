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

import org.apache.calcite.tools.Frameworks;
import org.apache.samza.sql.schema.CalciteModelProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

public class TestSamzaSQLTableFactory {
  public static final String STREAM_SCHEMA = "{\n"
      + "       name: 'KAFKA',\n"
      + "       tables: [ {\n"
      + "         type: 'custom',\n"
      + "         name: 'ORDERS',\n"
      + "         stream: {\n"
      + "           stream: true\n"
      + "         },\n"
      + "         factory: '" + SamzaSQLTableFactory.class.getName() + "',\n"
      + "         operand: {\n"
      + "           messageschema: {\n"
      + "             type: 'record',\n"
      + "             namespace: 'org.apache.samza.sql',\n"
      + "             \"fields\" : [{\"name\" : \"age\", \"type\" : \"int\", \"default\" : -1}]"
      + "           }\n"
      + "         }\n"
      + "       }]\n"
      + "}";

  public static final String STREAM_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'STREAMS',\n"
      + "   schemas: [\n"
      + STREAM_SCHEMA
      + "   ]\n"
      + "}";

  @Test
  public void testMessageSchema() throws IOException, SQLException {
    CalciteModelProcessor modelProcessor = new CalciteModelProcessor("inline:" + STREAM_MODEL, Frameworks.createRootSchema(true));
    Assert.assertNotNull(modelProcessor.getDefaultSchema());
  }
}

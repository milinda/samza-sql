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

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.metastore.SamzaSQLMetaStoreFactory;
import org.apache.samza.sql.metastore.ZKBakedQueryMetaStoreFactory;

public class SamzaSQLUtils {
  private static final String METADATA_STORE_FACTORY = "samza.sql.metadata.store.factory";

  public static SamzaSQLMetaStoreFactory getMetaStoreFactoryInstance(Config config) {
    String factoryClassName = config.get(METADATA_STORE_FACTORY, ZKBakedQueryMetaStoreFactory.class.toString());
    try {
      //Class factoryClass = Class.forName(factoryClassName);
      return new ZKBakedQueryMetaStoreFactory(); // TODO: Fix the class loader issue
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }
}

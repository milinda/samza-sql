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

import org.apache.samza.config.MapConfig;
import org.apache.samza.job.JobRunner;
import org.apache.samza.sql.master.model.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Query manager takes care of executing and managing the life-cycle of running
 * queries.
 */
public enum QueryManager {
  INSTANCE;

  private static final Logger log = LoggerFactory.getLogger(QueryManager.class);

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  public void executeQuery(ExecQuery execQuery) {
    executorService.execute(execQuery);
  }

  public static class ExecQuery implements Runnable {

    final Query query;

    public ExecQuery(Query query) {
      this.query = query;
    }

    @Override
    public void run() {
      new JobRunner(new MapConfig()).run();
      // Planning and submitting job should go here.

      /*
       * Job submission can be done via Samza's JobRunner
       *  - Generate job config and execute run-job.sh script
       *  - So we have to pack Samza with Master
       *  - Also we need to serve job archive via SamzaSQL master
       *  - Generate the config file inside a directory specific to the query
       *  - We need to have YARN cluster running with Http file system
       */
    }
  }
}

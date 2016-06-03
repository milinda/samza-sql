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


import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.JobRunner;
import org.apache.samza.job.StreamJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Wraps Samza's JobRunner to work on a map of configurations.
 */
public class SamzaSQLJobRunner{
  private static final Logger log = LoggerFactory.getLogger(SamzaSQLJobRunner.class);

  private final Map<String, String> config;

  public SamzaSQLJobRunner(Map<String, String> config) {
    this.config = config;
  }

  public StreamJob run(boolean resetJobConfig) {
    return new JobRunner(new JobConfig(new MapConfig(config))).run(resetJobConfig);
//    JobConfig jobConfig = new JobConfig(new MapConfig(config));
//
//    Option<String> jobFactoryClass = jobConfig.getStreamJobFactoryClass();
//
//    if (jobFactoryClass.isEmpty()) {
//      throw new SamzaException("no job factory class defined");
//    }
//
//    log.info("job factory: " + jobFactoryClass.get());
//
//    StreamJobFactory jobFactory;
//
//    try {
//      jobFactory = (StreamJobFactory) Class.forName(jobFactoryClass.get()).newInstance();
//    } catch (Exception e) {
//      throw new SamzaException("cannot instantiate job factory", e);
//    }
//
//    CoordinatorStreamSystemFactory coordinatorSystemFactory = new CoordinatorStreamSystemFactory();
//    CoordinatorStreamSystemConsumer coordinatorSystemConsumer =
//        coordinatorSystemFactory.getCoordinatorStreamSystemConsumer(jobConfig, new MetricsRegistryMap());
//    CoordinatorStreamSystemProducer coordinatorSystemProducer =
//        coordinatorSystemFactory.getCoordinatorStreamSystemProducer(jobConfig, new MetricsRegistryMap());
//
//    log.info("Creating coordinator stream");
//    Tuple2<SystemStream, SystemFactory> coordinatorSystemStreamAndFactory =
//        Util.getCoordinatorSystemStreamAndFactory(jobConfig);
//    SystemAdmin systemAdmin =
//        coordinatorSystemStreamAndFactory._2().getAdmin(coordinatorSystemStreamAndFactory._1().getSystem(), jobConfig);
//    systemAdmin.createCoordinatorStream(coordinatorSystemStreamAndFactory._1().getStream());
//
//    if(resetJobConfig) {
//      log.info("");
//    }
  }
}

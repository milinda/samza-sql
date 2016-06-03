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

package org.apache.samza.sql.physical;

public class JobConfigurations {
  public static final String JOB_FACTORY_CLASS  = "job.factory.class";
  public static final String JOB_NAME = "job.name";
  public static final String JOB_COORDINATOR_SYSTEM = "job.coordinator.system";
  public static final String JOB_COORDINATOR_REPLICATION_FACTOR = "job.coordinator.replication.factor";
  public static final String TASK_CLASS = "task.class";
  public static final String TASK_INPUTS = "task.inputs";
  public static final String TASK_CHECKPOINT_FACTORY = "task.checkpoint.factory";
  public static final String TASK_COMMIT_MS = "task.commit.ms";
  public static final String SYSTEMS_SAMZA_FACTORY = "systems.%s.samza.factory";
  public static final String SYSTEMS_SAMZA_KEY_SERDE = "systems.%s.samza.key.serde";
  public static final String SYSTEMS_STREAMS_SAMZA_KEY_SERDE = "systems.%s.streams.%s.samza.key.serde";
  public static final String SYSTEMS_SAMZA_MSG_SERDE = "systems.%s.samza.msg.serde";
  public static final String SYSTEMS_STREAMS_SAMZA_MSG_SERDE = "systems.%s.streams.%s.samza.msg.serde";
  public static final String SYSTEMS_SAMZA_OFFSET_DEFAULT = "systems.%s.samza.offset.default";
  public static final String SYSTEMS_STREAMS_SAMZA_OFFSET_DEFAULT = "systems.%s.streams.%s.samza.offset.default";
  public static final String SYSTEMS_STREAMS_SAMZA_BOOTSTRAP = "systems.%s.streams.%s.samza.bootstrap";
  public static final String TASK_CONSUMER_BATCH_SIZE = "task.consumer.batch.size";
  public static final String SERIALIZERS_REGISTRY_CLASS = "serializers.registry.%s.class";
  public static final String SYSTEMS_CONSUMER_ZOOKEEPER_CONNECT = "systems.%s.consumer.zookeeper.connect";
  public static final String SYSTEMS_PRODUCER_METADATA_BROKER_LIST = "systems.%s.producer.bootstrap.servers";
  public static final String SYSTEMS_PRODUCER_PRODUCER_TYPE = "systems.%s.producer.producer.type";
  public static final String SYSTEMS_FETCH_THRESHOLD = "systems.%s.samza.fetch.threshold";
  public static final String TASK_CHECKPOINT_SYSTEM = "task.checkpoint.system";
  public static final String STORES_FACTORY = "stores.%s.factory";
  public static final String STORES_KEY_SERDE = "stores.%s.key.serde";
  public static final String STORES_MSG_SERDE = "stores.%s.msg.serde";
  public static final String STORES_CHANGELOG = "stores.%s.changelog";
  public static final String YARN_PACKAGE_PATH = "yarn.package.path";
  public static final String YARN_CONTAINER_COUNT = "yarn.container.count";
  public static final String YARN_CONTAINER_MEMORY_MB = "yarn.container.memory.mb";
  public static final String YANR_CONTAINER_CPU_CORES = "yarn.container.cpu.cores";
  public static final String METRICS_REPORTER_CLASS = "metrics.reporter.%s.class";
  public static final String METRICS_REPOSTERS = "metrics.reporters";
  public static final String METRICS_REPORTER_STREAM = "metrics.reporter.%s.stream";
  public static final String SERIALIZERS_SCHEMA = "serializers.%s.schema";
  public static final String CALCITE_MODEL = "calcite.model";
  public static final String METADATA_STORE_FACTORY = "samza.sql.metadata.store.factory";

}

/**
 * Copyright 2016 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.samza.config;

import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.task.StreamTask;

import java.util.List;

public class TaskConfiguration {
  private final Class<? extends StreamTask> taskClass;
  private final List<StreamConfiguration> inputs;
  private final List<StreamPartitionConfiguration> broadcastInputs;
  private final Class<? extends CheckpointManagerFactory> checkpointFactory;
  private int consumerBatchSize;

  public TaskConfiguration(Class<? extends StreamTask> taskClass,
                           List<StreamConfiguration> inputs,
                           List<StreamPartitionConfiguration> broadcastInputs,
                           Class<? extends CheckpointManagerFactory> checkpointFactory) {
    this.taskClass = taskClass;
    this.inputs = inputs;
    this.broadcastInputs = broadcastInputs;
    this.checkpointFactory = checkpointFactory;
  }

  public Class<? extends StreamTask> getTaskClass() {
    return taskClass;
  }

  public List<StreamConfiguration> getInputs() {
    return inputs;
  }

  public List<StreamPartitionConfiguration> getBroadcastInputs() {
    return broadcastInputs;
  }

  public Class<? extends CheckpointManagerFactory> getCheckpointFactory() {
    return checkpointFactory;
  }

  public int getConsumerBatchSize() {
    return consumerBatchSize;
  }

  public void setConsumerBatchSize(int consumerBatchSize) {
    this.consumerBatchSize = consumerBatchSize;
  }
}

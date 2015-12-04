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

package org.apache.samza.sql.physical

import org.apache.samza.checkpoint.OffsetManager
import org.apache.samza.config.MapConfig
import org.apache.samza.container.{SamzaContainerContext, TaskInstance, TaskInstanceMetrics, TaskName}
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system._
import org.apache.samza.system.chooser.RoundRobinChooser
import org.apache.samza.task.{ReadableCoordinator, StreamTask, TaskInstanceCollector}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * In memory Samza task executor that can be used to test physical plans for streaming queries
 * @param task The {@link StreamTask} instance
 * @param taskName stream task's name
 * @param systemStream
 * @param systemStreamPartition
 */
class InMemoryStreamingTaskExecutor(task: StreamTask, taskName: String,
                                    systemStream: SystemStream,
                                    systemStreamPartition: SystemStreamPartition) {

  val name = new TaskName(taskName)
  val instanceCollector = new MockTaskInstanceCollector(systemProducers =
    new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager))
  val consumerMultiplexer = new SystemConsumers(new RoundRobinChooser, Map[String, SystemConsumer]())
  val config = new MapConfig
  val testSystemStreamMetadata = new SystemStreamMetadata(systemStream.getStream,
    Map(systemStreamPartition.getPartition -> new SystemStreamPartitionMetadata(null, null, "0")))
  val offsetManager = OffsetManager(Map(systemStream -> testSystemStreamMetadata), config)
  val containerContext = new SamzaContainerContext(0, config, Set[TaskName](name))
  val taskInstance = new TaskInstance(
    task,
    name,
    config,
    new TaskInstanceMetrics,
    consumerMultiplexer,
    instanceCollector,
    offsetManager)

  val coordinator = new ReadableCoordinator(name)

  def send(envelop: IncomingMessageEnvelope): Unit = {
    taskInstance.process(envelop, coordinator)
  }

  def outputTopics: scala.collection.Set[String] = {
    instanceCollector.getTopics
  }

  def topicContent(topic: String): ArrayBuffer[AnyRef] = {
    instanceCollector.getMessages(topic)
  }

  def getLastProcessedOffset: Option[String] = {
    offsetManager.getLastProcessedOffset(systemStreamPartition)
  }
}

class MockTaskInstanceCollector(systemProducers: SystemProducers, metrics: TaskInstanceMetrics = new TaskInstanceMetrics) extends TaskInstanceCollector(systemProducers, metrics) {
  var messages: mutable.Map[String, ArrayBuffer[AnyRef]] = scala.collection.mutable.Map[String, ArrayBuffer[AnyRef]]()

  /**
   * Register as a new source with SystemProducers. This allows this collector
   * to send messages to the SystemProducers.
   */
  override def register {}

  /**
   * Sends a message to the underlying SystemProducers.
   *
   * @param envelope An outgoing envelope that's to be sent to SystemProducers.
   */
  override def send(envelope: OutgoingMessageEnvelope): Unit = {
    val topic = envelope.getSystemStream.getStream

    if (messages.contains(topic)) {
      messages(topic) += envelope.getMessage
    } else {
      messages(topic) = ArrayBuffer(envelope.getMessage)
    }
  }

  def getTopics: scala.collection.Set[String] = {
    messages.keySet
  }

  def getMessages(topic: String): ArrayBuffer[AnyRef] = {
    messages(topic)
  }

  /**
   * Flushes the underlying SystemProducers.
   */
  override def flush {}
}

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

import java.text.SimpleDateFormat
import java.util
import java.util.UUID

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.samza.Partition
import org.apache.samza.config.{Config, MapConfig}
import org.apache.samza.container.{SamzaContainerContext, TaskName}
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.sql.data.avro.{AvroData, AvroSchema}
import org.apache.samza.sql.planner.TestQueryPlanner
import org.apache.samza.sql.test.MockSqlTask
import org.apache.samza.system._
import org.apache.samza.task._
import org.junit.Assert._
import org.junit.Test

import scala.io.Source

class TestPhysicalPlan {

  val ordersAvroSchemaSource = Source.fromURL(getClass.getResource("/orders.avsc"))
  val ordersAvroSchemaStr = try ordersAvroSchemaSource.mkString finally ordersAvroSchemaSource.close()
  val ordersAvroSchema = new Parser().parse(ordersAvroSchemaStr)
  val filterOrders = TestQueryPlanner.SIMPLE_FILTER
  val slidingWindowSum = TestQueryPlanner.SIMPLE_WINDOW_AGGREGATE
  val slidingWindowConfig: Config = new MapConfig()
  val calciteModel = TestQueryPlanner.STREAM_MODEL

  @Test
  def testQueryExecutor() {
    val task = new StreamTask {
      override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
        collector.send(new OutgoingMessageEnvelope(new SystemStream("test-system", "output-stream"),
          UUID.randomUUID().toString))
      }
    }
    val partition = new Partition(0)
    val systemStream = new SystemStream("test-system", "input-stream")
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val taskExecutor = new InMemoryStreamingTaskExecutor(task, "simple-task", systemStream, systemStreamPartition)

    // Make sure to increment the message offset manually when using task executor.
    taskExecutor.send(new IncomingMessageEnvelope(systemStreamPartition, "0", null, UUID.randomUUID().toString))
    taskExecutor.send(new IncomingMessageEnvelope(systemStreamPartition, "1", null, UUID.randomUUID().toString))

    val lastProcessedOffset = taskExecutor.getLastProcessedOffset
    assertTrue(lastProcessedOffset.isDefined)
    assertEquals("1", lastProcessedOffset.get)
    assertEquals(2, taskExecutor.topicContent("output-stream").size)
  }

  @Test
  def testSimpleFilter() {
    val filterTask = new MockSqlTask(calciteModel, filterOrders)
    val partition = new Partition(0)
    val systemStream = new SystemStream("kafka", "orders")
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val filterExecutor = new InMemoryStreamingTaskExecutor(filterTask, "sql-task", systemStream, systemStreamPartition)
    val orders = loadOrderRecords()

    orders.indices.foreach((i: Int) => {
      filterExecutor.send(new IncomingMessageEnvelope(systemStreamPartition, i.toString, null, orders(i)))
    })

    assertEquals(filterExecutor.outputTopics.size, 1)
    assertEquals(filterExecutor.topicContent(filterExecutor.outputTopics.head).size, 3)
  }

  @Test
  def testTimeBasedSlidingWindow(): Unit = {
    val slidingWindowTask = new MockSqlTask(calciteModel, slidingWindowSum)
    slidingWindowTask.init(slidingWindowConfig, new TaskContext {
      override def setStartingOffset(ssp: SystemStreamPartition, offset: String): Unit = ???

      override def getMetricsRegistry: MetricsRegistry = ???

      override def getTaskName: TaskName = ???

      override def getStore(name: String): AnyRef = {
        return Nil
      }

      override def getSamzaContainerContext: SamzaContainerContext = ???

      override def getSystemStreamPartitions: util.Set[SystemStreamPartition] = ???
    })
    val partition = new Partition(0)

    val systemStream = new SystemStream("kafka", "orders")
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val slidingWindowExecutor = new InMemoryStreamingTaskExecutor(slidingWindowTask, "sql-task", systemStream, systemStreamPartition)
    val orders = loadOrderRecords()

    orders.indices.foreach((i: Int) => {
      slidingWindowExecutor.send(new IncomingMessageEnvelope(systemStreamPartition, i.toString, null, orders(i)))
    })


    println(s"Output size: ${slidingWindowExecutor.outputTopics.size}")

    /*
     * It looks like this way of executing tasks can't be use with window operators due to the requirements of local stores.
     * So we need to try to use ThreadJobFactory instead.
     *
     * 1. We need in-memory system which is a singleton
     * 2. We need
     */
  }

  private def loadOrderRecords(): List[AvroData] = {
    val testDataSource = Source.fromURL(getClass.getResource("/test-data.csv"))
    val recordItr = testDataSource.getLines().drop(1).map(_.split(","))

    val avroRecordItr = recordItr.map((r: Array[String]) => {
      val recordBuilder = new GenericRecordBuilder(ordersAvroSchema)
      val timeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      val eventTime = timeFormat.parse(r(0))
      recordBuilder.set("rowtime", eventTime.getTime)
      recordBuilder.set("productId", r(1))
      recordBuilder.set("orderId", r(2).toInt)
      recordBuilder.set("units", r(3).toInt)

      val record = recordBuilder.build()
      AvroData.getStruct(AvroSchema.getSchema(ordersAvroSchema), record)
    })

    avroRecordItr.toList
  }
}

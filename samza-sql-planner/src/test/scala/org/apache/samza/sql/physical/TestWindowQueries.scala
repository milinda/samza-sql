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

import java.util
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, TestZKUtils, Utils, ZKStringSerializer}
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.calcite.tools.Frameworks
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.samza.SamzaException
import org.apache.samza.config.{Config, KafkaProducerConfig, MapConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.job.local.ThreadJobFactory
import org.apache.samza.job.{ApplicationStatus, StreamJob}
import org.apache.samza.sql.api.operators.OperatorRouter
import org.apache.samza.sql.data.IncomingMessageTuple
import org.apache.samza.sql.planner.QueryPlanner
import org.apache.samza.sql.planner.logical.SamzaRel
import org.apache.samza.sql.schema.CalciteModelProcessor
import org.apache.samza.sql.test.MockQueryContext
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.kafka.TopicMetadataCache
import org.apache.samza.task._
import org.apache.samza.util.{ClientUtilTopicMetadataStore, KafkaUtil, TopicMetadataStore}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random


object TestWindowQueries {
  val logger = LoggerFactory.getLogger(classOf[TestWindowQueries])
  val INPUT_TOPIC = "orders"
  val MESSAGE_STORE = "messagestore"
  val WINDOW_STORE = "windowstore"
  val TOTAL_TASK_NAMES = 1
  val REPLICATION_FACTOR = 1

  val SLIDING_WINDOW_SUM_ID = "sliding-sum"
  val SLIDING_WINDOW_SUM =
    """SELECT STREAM rowtime,
                              productId,
                              units,
                              SUM(units) OVER (PARTITION BY productId ORDER BY rowtime RANGE INTERVAL '2' MINUTE PRECEDING) unitsLastHour
                              FROM Orders""".stripMargin
  val ORDER_SCHEMA =
    """
      inline:{
        name: 'KAFKA',
        tables: [ {
          type: 'custom',
          name: 'ORDERS',
          stream: {
            stream: true
          },
          factory: 'org.apache.samza.sql.test.OrderStreamFactory'
        }, {
          type: 'custom',
          name: 'AGGREGATEDORDERS',
          stream: {
            stream: true
          },
          factory: 'org.apache.samza.sql.test.AggregatedOrderStreamFactory'
        }
      }
    """.stripMargin

  val ordersAvroSchema = SchemaBuilder
    .record("orders")
    .fields()
    .name("orderId").`type`().intType().noDefault()
    .name("productId").`type`().stringType().noDefault()
    .name("units").`type`().intType().noDefault()
    .name("rowtime").`type`().longType().noDefault()
    .endRecord()
  val orderId = new AtomicInteger()
  val rowTime = new AtomicLong(System.currentTimeMillis())
  val productIds = Array("paper", "pencil", "glue", "tape")
  val randomNumberGenerator = new Random(System.currentTimeMillis())


  val zkConnect: String = TestZKUtils.zookeeperConnect
  var zkClient: ZkClient = null
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  val brokerId1 = 0
  val ports = TestUtils.choosePorts(1)
  val port1 = ports(0)

  val props1 = TestUtils.createBrokerConfig(brokerId1, port1)
  props1.setProperty("auto.create.topics.enable", "false")

  val config = new util.HashMap[String, Object]()
  val brokers = "localhost:%d" format (port1)
  config.put("bootstrap.servers", brokers)
  config.put("request.required.acks", "-1")
  config.put("serializer.class", "kafka.serializer.StringEncoder")
  config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, new Integer(1))
  config.put(ProducerConfig.RETRIES_CONFIG, new Integer(Integer.MAX_VALUE - 1))
  val producerConfig = new KafkaProducerConfig("kafka", "i001", config)
  var producer: Producer[Array[Byte], Array[Byte]] = null
  var zookeeper: EmbeddedZookeeper = null
  var server1: KafkaServer = null
  var metadataStore: TopicMetadataStore = null
  var ordersSchemaId: Integer = null

  val schemaRegistryConfigMap = Map("port" -> "8081",
    "kafkastore.connection.url" -> brokers,
    "kafkastore.topic" -> "_schemas",
    "debug" -> "false")
  val schemaRegistryUrl = "http://localhost:8081"

  @BeforeClass
  def beforeSetupServers: Unit = {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    server1 = TestUtils.createServer(new KafkaConfig(props1))
    zkClient = new ZkClient(zkConnect + "/", 6000, 6000, ZKStringSerializer)
    producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")

    createSchemaRegistry
    registerSchemas
    createTopics
    validateTopics
  }

  def createSchemaRegistry: Unit = {
    val schemaRegistryConfig = new SchemaRegistryConfig(schemaRegistryConfigMap)
    val server = new SchemaRegistryRestApplication(schemaRegistryConfig).createServer()
    server.start()
    logger.info("Schema registry started, listening for requests..")
  }

  def registerSchemas: Unit = {
    val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000)
    ordersSchemaId = schemaRegistryClient.register("orders", ordersAvroSchema)
  }

  def createTopics {
    AdminUtils.createTopic(
      zkClient,
      INPUT_TOPIC,
      TOTAL_TASK_NAMES,
      REPLICATION_FACTOR)
  }

  def validateTopics {
    val topics = Set(INPUT_TOPIC)
    var done = false
    var retries = 0

    while (!done && retries < 100) {
      try {
        val topicMetadataMap = TopicMetadataCache.getTopicMetadata(topics, "kafka", metadataStore.getTopicInfo)

        topics.foreach(topic => {
          val topicMetadata = topicMetadataMap(topic)
          val errorCode = topicMetadata.errorCode

          KafkaUtil.maybeThrowException(errorCode)
        })

        done = true
      } catch {
        case e: Exception =>
          System.err.println("Got exception while validating test topics. Waiting and retrying.", e)
          retries += 1
          Thread.sleep(500)
      }
    }

    if (retries >= 100) {
      fail("Unable to successfully create topics. Tried to validate %s times." format retries)
    }
  }

  @AfterClass
  def afterCleanLogDirs: Unit = {
    producer.close()
    server1.shutdown
    server1.awaitShutdown()
    Utils.rm(server1.config.logDirs)
    zkClient.close
    zookeeper.shutdown
  }

  def createRandomOrder(): GenericRecord = {
    new GenericRecordBuilder(ordersAvroSchema)
      .set("orderId", orderId.getAndIncrement())
      .set("productId", productIds(randomNumberGenerator.nextInt(productIds.length)))
      .set("units", randomNumberGenerator.nextInt(50))
      .set("rowtime", rowTime.getAndAdd(randomNumberGenerator.nextLong()))
      .build()
  }
}

class TestWindowQueries {

  import TestWindowQueries._

  val jobFactory = new ThreadJobFactory


  def jobConfig(query: String): Config = {
    val jobConfig = Map(
      "job.factory.class" -> jobFactory.getClass.getCanonicalName,
      "job.name" -> "hello-stateful-world",
      "task.class" -> "org.apache.samza.test.integration.TestTask",
      "task.inputs" -> "kafka.input",
      "serializers.registry.string.class" -> "org.apache.samza.serializers.StringSerdeFactory",
      "stores.mystore.factory" -> "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory",
      "stores.mystore.key.serde" -> "string",
      "stores.mystore.msg.serde" -> "string",
      "stores.mystore.changelog" -> "kafka.mystoreChangelog",
      "stores.mystore.changelog.replication.factor" -> "1",
      "systems.kafka.samza.factory" -> "org.apache.samza.system.kafka.KafkaSystemFactory",
      // Always start consuming at offset 0. This avoids a race condition between
      // the producer and the consumer in this test (SAMZA-166, SAMZA-224).
      "systems.kafka.samza.offset.default" -> "oldest", // applies to a nonempty topic
      "systems.kafka.consumer.auto.offset.reset" -> "smallest", // applies to an empty topic
      "systems.kafka.samza.msg.serde" -> "string",
      "systems.kafka.consumer.zookeeper.connect" -> zkConnect,
      "systems.kafka.producer.bootstrap.servers" -> ("localhost:%s" format port1),
      // Since using state, need a checkpoint manager
      "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
      "task.checkpoint.system" -> "kafka",
      "task.checkpoint.replication.factor" -> "1",
      // However, don't have the inputs use the checkpoint manager
      // since the second part of the test expects to replay the input streams.
      "systems.kafka.streams.input.samza.reset.offset" -> "true",
      "sql.query" -> query)

    return new MapConfig(jobConfig)
  }

  /**
    * Start a job for SQL query execution, and do basic verifications.
    */
  def startJob(query: String) = {
    val job = jobFactory.getJob(jobConfig(query))

    // Start job
    job.submit()
    assertEquals(ApplicationStatus.Running, job.waitForStatus(ApplicationStatus.Running, 60000))
    SqlTask.awaitTaskRegistered
    val tasks = SqlTask.tasks

    assertEquals("Should only have a single partition in this task", 1, tasks.size)

    val task = tasks.values.toList.head

    task.initFinished.await(60, TimeUnit.SECONDS)
    assertEquals(0, task.initFinished.getCount)

    (job, task)
  }

  def stopJob(job: StreamJob): Unit = {
    job.kill()
    assertEquals(ApplicationStatus.UnsuccessfulFinish, job.waitForFinish(60000))
  }

  // Send a message to the input topic, and validate that it gets to the test task
  def send(task: SqlTask): Unit = {

  }

  @Test
  def testTimeBasedSlidingWindowSum: Unit = {
    val (job, task) = startJob(SLIDING_WINDOW_SUM_ID)

    // Validate that the task has initialized
    assertEquals(0, task.initFinished.getCount)

    // Send some messages to input stream
    // TODO: Validate the message store and window store
    // Read messages from the output stream
  }
}

object SqlTask {
  val tasks = new mutable.HashMap[TaskName, SqlTask] with mutable.SynchronizedMap[TaskName, SqlTask]
  @volatile var allTasksRegistered = new CountDownLatch(TestWindowQueries.TOTAL_TASK_NAMES)

  def register(taskName: TaskName, task: SqlTask): Unit = {
    tasks += taskName -> task
    allTasksRegistered.countDown()
  }

  def awaitTaskRegistered: Unit = {
    allTasksRegistered.await(60, TimeUnit.SECONDS)
    assertEquals(0, allTasksRegistered.getCount)
    assertEquals(TestWindowQueries.TOTAL_TASK_NAMES, tasks.size)
    // Reset the registered latch, so we can use it again every time we start a new job.
    SqlTask.allTasksRegistered = new CountDownLatch(TestWindowQueries.TOTAL_TASK_NAMES)
  }
}

class SqlTask extends StreamTask with InitableTask {
  val queryMap = Map(TestWindowQueries.SLIDING_WINDOW_SUM_ID -> TestWindowQueries.SLIDING_WINDOW_SUM)
  val initFinished = new CountDownLatch(1)
  var gotMessage = new CountDownLatch(1)
  var operatorRouter: Option[OperatorRouter] = None

  override def init(config: Config, context: TaskContext): Unit = {
    SqlTask.register(context.getTaskName, this)
    val rootSchema = Frameworks.createRootSchema(true)
    val queryContext = new MockQueryContext(new CalciteModelProcessor(TestWindowQueries.ORDER_SCHEMA, rootSchema).getDefaultSchema)
    val queryPlanner = new QueryPlanner(queryContext)
    operatorRouter = Some(queryPlanner.getPhysicalPlan(queryPlanner.getPlan(config.get("sql.query", TestWindowQueries.SLIDING_WINDOW_SUM_ID)).asInstanceOf[SamzaRel]))
    if (operatorRouter.isEmpty) {
      throw new SamzaException("Uninitialized operator router.")
    }
    operatorRouter.get.init(config, context)
    initFinished.countDown()
  }

  override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
    if (operatorRouter.isEmpty) {
      throw new SamzaException("Uninitialized operator router.")
    }
    operatorRouter.get.process(new IncomingMessageTuple(envelope), collector, coordinator)
  }


}

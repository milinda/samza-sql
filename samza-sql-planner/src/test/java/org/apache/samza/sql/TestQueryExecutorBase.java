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

import com.google.common.io.Resources;
import com.google.gson.Gson;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.SamzaException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.*;

public abstract class TestQueryExecutorBase {
  protected static ZkClient zkClient;
  protected static EmbeddedZookeeper zkServer;
  protected static int brokerId = 0;
  protected static int brokerPort = TestUtils.choosePort();
  protected static String brokers = String.format("localhost:%s", brokerPort);
  protected static KafkaServer kafkaServer;
  protected static List<KafkaServer> kafkaServers;

  @BeforeClass
  public static void setup() {
    zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    kafkaServer = TestUtils.createServer(new KafkaConfig(TestUtils.createBrokerConfig(brokerId, brokerPort, true)),
        new MockTime());
    kafkaServers = new ArrayList<>();
    kafkaServers.add(kafkaServer);
  }

  @AfterClass
  public static void teardown() {
    kafkaServer.shutdown();
    zkServer.shutdown();
  }

  protected void createTopic(String topic, int partitions) {
    String[] args = new String[]{"--topic", topic, "--partitions", String.valueOf(partitions), "--replication-factor",
        "1"};
    TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(args));
    waitForTopic(topic);
  }

  protected void waitForTopic(String topic) {
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(kafkaServers), topic, 0,
        30000);
  }

  protected void deleteTopic(String topic) {
    TopicCommand.deleteTopic(zkClient, new TopicCommand.TopicCommandOptions(new String[]{"--topic", topic}));
  }

  protected void publish(String topic, List<KeyedMessage> messages) {
    // TODO: Explore how to allow custom partitioning
    Producer producer = new Producer(new ProducerConfig(TestUtils.getProducerConfig(brokers)));
    producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
    producer.close();
  }

  protected List<Map<Object, Object>> readMessages(String path, String type) throws IOException {
    Gson gson = new Gson();
    Map<Object, Object> testData = (Map<Object, Object>) gson.fromJson(
        Resources.toString(TestQueryExecutor.class.getResource(path), Charset.defaultCharset()),
        Map.class);
    if (testData.containsKey(type)) {
      return (List<Map<Object, Object>>) testData.get(type);
    } else {
      throw new SamzaException("Invalid test data format. Cannot find input messages.");
    }
  }

  protected String resourceToString(String path) throws IOException {
    return Resources.toString(TestQueryExecutorBase.class.getResource(path), Charset.defaultCharset());
  }

  protected void deleteConsumerGroup(String group) {
    zkClient.delete(String.format("/consumers/%s", group));
  }

  protected void verify(String topic, String consumerGroup, QueryOutputVerifier verifier) throws Exception {
    Properties consumerProperties = TestUtils.createConsumerProperties(zkServer.connectString(), consumerGroup, "consumer0", -1);
    ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);

    verifier.verify(stream);
  }

  protected String salesSchema(String zkConnectionStr, String brokers) throws IOException, TemplateException {
    Configuration cfg = new Configuration(Configuration.VERSION_2_3_23);
    cfg.setDefaultEncoding("UTF-8");
    cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    cfg.setClassForTemplateLoading(this.getClass(), "/");
    Map root = new HashMap();
    root.put("brokers", brokers);
    root.put("zkConnecitonStr", zkConnectionStr);
    Template template = cfg.getTemplate("sales.ftl");
    Writer out = new StringWriter(4096);
    template.process(root, out);

    return out.toString();
  }

  public static interface QueryOutputVerifier {
    void verify(KafkaStream<byte[], byte[]> stream) throws Exception;
  }
}

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

package org.apache.samza.sql.bench.utils;

import com.google.common.io.Resources;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataVerifier {
  private static final Logger log = LoggerFactory.getLogger(DataVerifier.class);

  public enum SchemaType {
    ORDERS,
    PROJECT,
    FILTER,
    SLIDINGWINDOW,
    JOIN;
  }

  private final String[] args;
  private final Options options = new Options();
  private String zkConnectionString = "localhost:2181";
  private String consumerGroupId;
  private String topic;

  public DataVerifier(String[] args) {
    this.args = args;
    options.addOption("v", true, "Verify (ORDERS | PROJECT | FILTER | SLIDINGWINDOW | JOIN)");
    options.addOption("t", true, "Topic");
    options.addOption("z", true, "Zookeeper Conneciton String");
    options.addOption("g", true, "Consumer Group ID");
  }

  public void execute() {
    CommandLineParser cliParser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = cliParser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException("Cannot parse command line arguments: " + Arrays.toString(args));
    }

    if (cmd.hasOption('z')) {
      zkConnectionString = cmd.getOptionValue('z');
    }

    if (cmd.hasOption('g')) {
      consumerGroupId = cmd.getOptionValue('g');
    } else {
      throw new RuntimeException("Missing required parameter [-g] group id.");
    }

    if (cmd.hasOption('t')) {
      topic = cmd.getOptionValue('t').trim();
    } else {
      throw new RuntimeException("Missing required parameter [-t] topic.");
    }

    if (cmd.hasOption('v')) {

      SchemaType type = SchemaType.valueOf(cmd.getOptionValue('v').trim());

      try {
        System.out.printf("Start consuming: " + topic + " consumer group: " + consumerGroupId);
        VerifierConsumer verifierConsumer = new VerifierConsumer(zkConnectionString, consumerGroupId, topic, type);
        System.out.println("Running orders consumer.");
        verifierConsumer.run(2);
        Thread.sleep(600000);
        verifierConsumer.shutdown();
      } catch (Exception e) {
        log.error("Cannot consume orders.", e);
        System.exit(-1);
      }
    } else {
      throw new RuntimeException("Specify the verifier type (Orders | ProjectedOrders | SlidingWindow).");
    }
  }

  private static ConsumerConfig createConsumerConfig(String zkConnect, String groupId) {
    Properties props = new Properties();
    props.put("zookeeper.connect", zkConnect);
    props.put("group.id", groupId);
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    return new ConsumerConfig(props);
  }

  public static String loadSchema(SchemaType type) throws IOException {
    switch (type) {
      case ORDERS:
        return Resources.toString(TestDataGenerator.class.getResource("/benchorders.avsc"), Charset.defaultCharset());
      case PROJECT:
        return Resources.toString(TestDataGenerator.class.getResource("/projectout.avsc"), Charset.defaultCharset());
      case SLIDINGWINDOW:
        return Resources.toString(TestDataGenerator.class.getResource("/slidingwindowout.avsc"), Charset.defaultCharset());
      case FILTER:
        return Resources.toString(TestDataGenerator.class.getResource("/benchorders.avsc"), Charset.defaultCharset());
      case JOIN:
        return Resources.toString(TestDataGenerator.class.getResource("/joinout.avsc"), Charset.defaultCharset());
      default:
        throw new RuntimeException("Unknown verifier type: " + type);
    }
  }

  public static void main(String[] args) {
    new DataVerifier(args).execute();
  }

  public static class VerifierConsumer {
    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;
    private final SchemaType type;

    public VerifierConsumer(String zkConnect, String groupId, String topic, SchemaType type) {
      this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
          createConsumerConfig(zkConnect, groupId));
      this.topic = topic;
      this.type = type;
    }

    public void shutdown() {
      if (consumer != null) consumer.shutdown();
      if (executor != null) executor.shutdown();
      try {
        if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
          System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
        }
      } catch (InterruptedException e) {
        System.out.println("Interrupted during shutdown, exiting uncleanly");
      }
    }

    public void run(int numThreads) throws IOException {
      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
      topicCountMap.put(topic, new Integer(numThreads));
      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
      List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

      System.out.println("Streams size: " + streams.size());
      // now launch all the threads
      //
      executor = Executors.newFixedThreadPool(numThreads);

      // now create an object to consume the messages
      //
      int threadNumber = 0;
      for (final KafkaStream stream : streams) {
        executor.submit(new PrintVerifier(stream, threadNumber, type));
        threadNumber++;
      }
    }
  }

  public static class PrintVerifier implements Runnable {
    private KafkaStream stream;
    private int threadNumber;
    private final GenericDatumReader<GenericRecord> reader;
    private final SchemaType type;


    public PrintVerifier(KafkaStream stream, int threadNumber, SchemaType type) throws IOException {
      this.threadNumber = threadNumber;
      this.stream = stream;
      this.type = type;
      reader = new GenericDatumReader<GenericRecord>(new Schema.Parser().parse(DataVerifier.loadSchema(type)));
    }

    public void run() {
      System.out.println("Start consuming: " + threadNumber);
      ConsumerIterator<byte[], byte[]> it = stream.iterator();
      while (it.hasNext()) {
        GenericRecord record = null;
        try {
          record = reader.read(null, DecoderFactory.get().binaryDecoder(it.next().message(), null));
        } catch (IOException e) {
          log.error("Cannot read avro message.", e);
        }
        if (record != null) {
          switch (type){
            case ORDERS:
              System.out.println("Thread " + threadNumber + ": " + record.get("orderId") + ":" + record.get("units"));
              break;
            case PROJECT:
            case FILTER:
              System.out.println("Thread " + threadNumber + ": " + record.get("orderId") + ":" + record.get("units"));
              break;
            case JOIN:
              break;
            case SLIDINGWINDOW:
              System.out.println("Thread " + threadNumber + " productId: " + record.get("productId") + " unitsLastHour:" + record.get("unitsLastHour"));
              break;
          }
        }
      }
      System.out.printf("Shutting down Thread: " + threadNumber);
    }
  }
}

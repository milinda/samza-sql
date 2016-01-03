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
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TestDataGenerator {
  private static final Logger log = LoggerFactory.getLogger(TestDataGenerator.class);

  public static final int NUMBER_OF_RECORDS_DEFAULT = 80000000;
  public static final int NUMBER_OF_PRODUCTS_DEFAULT = 10000;
  //public static final String DEFAULT_KAFKA_BROKER = "localhost:9092";
  public static final String DEFAULT_KAFKA_BROKER = "ec2-52-33-96-187.us-west-2.compute.amazonaws.com:9092,ec2-52-34-207-208.us-west-2.compute.amazonaws.com:9092,ec2-52-32-175-164.us-west-2.compute.amazonaws.com:9092";
  public static final String DEFAULT_TOPIC = "orders";

  private final Options options = new Options();
  private final String[] args;
  private int numberOfRecords = NUMBER_OF_RECORDS_DEFAULT;
  private int numberOfProducts = NUMBER_OF_PRODUCTS_DEFAULT;
  private String kafkaBrokers = DEFAULT_KAFKA_BROKER;
  private String topic = DEFAULT_TOPIC;
  private ExecutorService executorService = Executors.newSingleThreadExecutor();


  public TestDataGenerator(String[] args) {
    this.args = args;
    options.addOption("p", false, "Generate Products Table");
    options.addOption("r", true, "Number of Records to Generate");
    options.addOption("n", true, "Number of Products");
    options.addOption("b", true, "Kafka Brokers");
    options.addOption("t", true, "Topic");
  }

  public void execute() throws IOException {
    CommandLineParser cliParser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = cliParser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException("Cannot parse command line arguments: " + Arrays.toString(args));
    }

    if (cmd.hasOption('r')) {
      numberOfRecords = Integer.valueOf(cmd.getOptionValue('r').trim());
    }

    if (cmd.hasOption('n')) {
      numberOfProducts = Integer.valueOf(cmd.getOptionValue('n').trim());
    }

    if (cmd.hasOption('b')) {
      kafkaBrokers = cmd.getOptionValue('b').trim();
    }

    if (cmd.hasOption('t')) {
      topic = cmd.getOptionValue('t').trim();
    }

    if (cmd.hasOption('p')) {
      log.info("Doesn't support Products table generation yet.");
      System.exit(0);
    }

    OrdersProducer ordersProducer = new OrdersProducer(numberOfRecords, numberOfProducts, kafkaBrokers, topic);
    executorService.execute(ordersProducer);
  }

  public static void main(String[] args) throws IOException {
    new TestDataGenerator(args).execute();
  }

  public static String loadOrdersSchema() throws IOException {
    return Resources.toString(TestDataGenerator.class.getResource("/benchorders.avsc"), Charset.defaultCharset());
  }

  public static class OrdersProducer implements Runnable {

    private final int numberOfRecords;
    private final int numberOfProducts;
    private final String brokers;
    private final Schema ordersSchema;
    private final Random rand = new Random(System.currentTimeMillis());
    private final RandomString randString = new RandomString(85);
    private final AtomicInteger orderId = new AtomicInteger(0);
    private final Producer<Integer, GenericRecord> producer;
    private final String topic;

    public OrdersProducer(int numberOfRecords, int numberOfProducts, String brokers, String topic) throws IOException {
      this.numberOfRecords = numberOfRecords;
      this.numberOfProducts = numberOfProducts;
      this.brokers = brokers;
      this.topic = topic;
      this.ordersSchema = new Schema.Parser().parse(loadOrdersSchema());
      this.producer = new Producer<Integer, GenericRecord>(new ProducerConfig(produceProperties()));
    }

    @Override
    public void run() {
      while (orderId.get() < numberOfRecords) {
        GenericRecord record = genProduct();
        int productId = (Integer) record.get("productId");
        producer.send(new KeyedMessage<Integer, GenericRecord>(topic, productId, record));
      }
      System.out.println("Done producing orders...");
    }

    private GenericRecord genProduct() {
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(ordersSchema);
      int productId = rand.nextInt(numberOfProducts);
      recordBuilder.set("orderId", orderId.getAndIncrement());
      recordBuilder.set("productId", productId);
      recordBuilder.set("units", rand.nextInt(200));
      recordBuilder.set("rowtime", System.currentTimeMillis());
      recordBuilder.set("padding", randString.nextString());
      return recordBuilder.build();
    }

    private Properties produceProperties() {
      Properties props = new Properties();

      props.put("metadata.broker.list", brokers);
      props.put("key.serializer.class", "org.apache.samza.sql.bench.utils.IntEncoder");
      props.put("serializer.class", "org.apache.samza.sql.bench.utils.OrdersEncoder");
      props.put("partitioner.class", "org.apache.samza.sql.bench.utils.KeyBasedPartitioner");
      props.put("producer.type", "sync");

      return props;
    }
  }

}

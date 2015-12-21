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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MetricsProcessor {


  private final ConsumerConnector consumer;
  private final String topic;
  private ExecutorService executor;
  private String influxDbHost;
  private final String experiment;

  public MetricsProcessor(String experiment, String a_zookeeper, String a_groupId, String a_topic, String influxDbHost) {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
        createConsumerConfig(a_zookeeper, a_groupId));
    this.topic = a_topic;
    this.influxDbHost = influxDbHost;
    this.experiment = experiment;
  }

  public void run(int a_numThreads) {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(a_numThreads));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

    // now launch all the threads
    //
    executor = Executors.newFixedThreadPool(a_numThreads);

    // now create an object to consume the messages
    //
    int threadNumber = 0;
    for (final KafkaStream stream : streams) {
      executor.submit(new ProcessMetrics(experiment, stream, threadNumber, influxDbHost));
      threadNumber++;
    }
  }

  private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
    Properties props = new Properties();
    props.put("zookeeper.connect", a_zookeeper);
    props.put("group.id", a_groupId);
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    return new ConsumerConfig(props);
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

  public static void main(String[] args) {
    String zooKeeper = "ec2-52-35-190-216.us-west-2.compute.amazonaws.com:2181";
    String groupId = "metricstest";
    String topic = "filtersqlmetrics2tasks";
    int threads = 2;

    MetricsProcessor metricsProcessor = new MetricsProcessor("filtersql", zooKeeper, groupId, topic, "");
    metricsProcessor.run(threads);

    try {
      Thread.sleep(300 * 1000);
    } catch (InterruptedException ie) {

    }
    metricsProcessor.shutdown();
  }

  public class ProcessMetrics implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private final String fileEnvelopsProcessed;
    private final String fileTaskProcessTime;

    public ProcessMetrics(String experiment, KafkaStream a_stream, int a_threadNumber, String influxDbHost) {
      m_threadNumber = a_threadNumber;
      m_stream = a_stream;
      fileEnvelopsProcessed = experiment + "-envelops.csv";
      fileTaskProcessTime = experiment + "-taskprocesstime.csv";
    }

    public void run() {
      ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
      while (it.hasNext()) {
        String msg = new String(it.next().message());
        System.out.println(msg);
        JsonElement metricsEvent = new JsonParser().parse(msg);
        JsonObject metricsEventObj = metricsEvent.getAsJsonObject();

        JsonObject headers = metricsEventObj.getAsJsonObject("header");
        String jobId = headers.get("job-id").getAsString();
        String container = headers.get("container-name").getAsString();
        long resetTime = headers.get("reset-time").getAsLong();
        long time = headers.get("time").getAsLong();
        String source = headers.get("source").getAsString();
        String jobName = headers.get("job-name").getAsString();

        JsonObject metrics = headers.getAsJsonObject("metrics");
        if (metrics.has("org.apache.samza.container.SamzaContainerMetrics")) {
          JsonObject containerMetrics = headers.getAsJsonObject("org.apache.samza.container.SamzaContainerMetrics");
          int processEnvelops = containerMetrics.get("process-envelopes").getAsInt();
          float taskProcessTime = containerMetrics.get("task-process-ms").getAsFloat();
          String serieNameRoot = jobName + "." + jobId + "." + source;
//            Serie envelopsPorcessed = new Serie.Builder(serieNameRoot + ".envelops.processed")
//                .columns("container", "reset-time", "time", "value")
//                .values(container, resetTime, time, processEnvelops)
//                .build();
//            Serie averageProcessTime = new Serie.Builder(serieNameRoot + ".task.process.time")
//                .columns("container", "reset-time", "time", "value")
//                .values(container, resetTime, time, taskProcessTime)
//                .build();
//            influxDB.write(db, TimeUnit.MILLISECONDS, envelopsPorcessed, averageProcessTime);
          System.out.println(String.format("Stats: %s %s %l %l %d", serieNameRoot + ".envelops.processed", container, resetTime, time, processEnvelops));
        }
      }


      System.out.println("Shutting down Thread: " + m_threadNumber);
    }
  }
}

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

package org.apache.samza.sql.bench;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class MetricsProcessorTask implements StreamTask, InitableTask {
  public static final String INFLUXDB_HOST = "samzasql.eval.influxdb.host";
  public static final String INFLUXDB_DB = "samzasql.eval.influxdb.db";

  private String influxDbHost;
  private InfluxDB influxDB;
  private String db;
  private Map<String, Integer> lastEnvelopsProcessed = new HashMap<String, Integer>();
  private Map<String, Integer> kvMetricLastValue = new HashMap<String, Integer>();
  private Map<String, Integer> cachedMetricLastValue = new HashMap<String, Integer>();

  public enum KVStoreMetric {
    GETS,
    PUTS,
    DELETES,
    RANGES,
    BYTESREAD,
    BYTESWRITTEN
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    influxDbHost = config.get(INFLUXDB_HOST, "localhost");
    db = config.get(INFLUXDB_DB, "samzasql-eval-" + System.currentTimeMillis());
    influxDB = InfluxDBFactory.connect("http://" + influxDbHost + ":8086", "root", "root");
    List<String> databases = influxDB.describeDatabases();

    if (!isDatabaseExists(db, databases)) {
      influxDB.createDatabase(db);
    }
  }

  private boolean isDatabaseExists(String dbName, List<String> databases) {
    for (String db : databases) {
      if (db.equals(dbName)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    Map<String, Object> metricsMsg = (Map<String, Object>) envelope.getMessage();
    Map<String, Object> header = (Map<String, Object>) metricsMsg.get("header");
    Map<String, Object> metrics = (Map<String, Object>) metricsMsg.get("metrics");

    String jobName = (String) header.get("job-name");
    String jobId = (String) header.get("job-id");
    String container = (String) header.get("container-name");
    String source = (String) header.get("source");
    long resetTime = (Long) header.get("reset-time");
    long time = (Long) header.get("time");
    String serieNameRoot = jobName + "." + jobId + "." + container;

    if (metrics.containsKey("org.apache.samza.container.SamzaContainerMetrics")) {
      Map<String, Object> containerMetrics = (Map<String, Object>) metrics.get("org.apache.samza.container.SamzaContainerMetrics");

      int processEnvelops = (Integer) containerMetrics.get("process-envelopes");
      double taskProcessTime = (Double) containerMetrics.get("process-ms");
      int envelopsProcessedInThisInterval = 0;
      if (processEnvelops != 0 && lastEnvelopsProcessed.containsKey(serieNameRoot)) {
        envelopsProcessedInThisInterval = processEnvelops - lastEnvelopsProcessed.get(serieNameRoot);
      }

      lastEnvelopsProcessed.put(serieNameRoot, processEnvelops);

      BatchPoints batchPoints = BatchPoints
          .database(db)
          .tag("async", "true")
          .retentionPolicy("default")
          .consistency(InfluxDB.ConsistencyLevel.ALL)
          .build();
      Point point1 = Point.measurement(serieNameRoot + ".envelops.processed")
          .time(time, TimeUnit.MILLISECONDS)
          .field("container", container)
          .field("reset-time", resetTime)
          .field("envelops-processed", envelopsProcessedInThisInterval)
          .build();
      Point point2 = Point.measurement(serieNameRoot + ".task.process.time")
          .time(time, TimeUnit.MILLISECONDS)
          .field("container", container)
          .field("reset-time", resetTime)
          .field("task-process-time", taskProcessTime)
          .build();
      batchPoints.point(point1);
      batchPoints.point(point2);
      influxDB.write(batchPoints);
      System.out.println(String.format("Stats: %s %s %d %d %d", serieNameRoot + ".envelops.processed", container, resetTime, time, processEnvelops));
    } else if (metrics.containsKey("org.apache.samza.storage.kv.KeyValueStoreMetrics")) {
      Map<String, Object> kvStoreMetrics = (Map<String, Object>) metrics.get("org.apache.samza.storage.kv.KeyValueStoreMetrics");
      publishKVStoreMetric(KVStoreMetric.GETS, kvStoreMetrics, serieNameRoot, time, resetTime);
      publishKVStoreMetric(KVStoreMetric.PUTS, kvStoreMetrics, serieNameRoot, time, resetTime);
      publishKVStoreMetric(KVStoreMetric.DELETES, kvStoreMetrics, serieNameRoot, time, resetTime);
      publishKVStoreMetric(KVStoreMetric.RANGES, kvStoreMetrics, serieNameRoot, time, resetTime);
      publishKVStoreMetric(KVStoreMetric.BYTESREAD, kvStoreMetrics, serieNameRoot, time, resetTime);
      publishKVStoreMetric(KVStoreMetric.BYTESWRITTEN, kvStoreMetrics, serieNameRoot, time, resetTime);
    } else if (metrics.containsKey("org.apache.samza.storage.kv.CachedStoreMetrics")) {
      Map<String, Object> kvStoreMetrics = (Map<String, Object>) metrics.get("org.apache.samza.storage.kv.CachedStoreMetrics");
      publishCachedStoreMetric(KVStoreMetric.GETS, kvStoreMetrics, serieNameRoot, time, resetTime);
      publishCachedStoreMetric(KVStoreMetric.PUTS, kvStoreMetrics, serieNameRoot, time, resetTime);
      publishCachedStoreMetric(KVStoreMetric.DELETES, kvStoreMetrics, serieNameRoot, time, resetTime);
      publishCachedStoreMetric(KVStoreMetric.RANGES, kvStoreMetrics, serieNameRoot, time, resetTime);
      publishCachedStoreMetric(KVStoreMetric.BYTESREAD, kvStoreMetrics, serieNameRoot, time, resetTime);
      publishCachedStoreMetric(KVStoreMetric.BYTESWRITTEN, kvStoreMetrics, serieNameRoot, time, resetTime);
    }


  }

  private void publishKVStoreMetric(KVStoreMetric metric, Map<String, Object> metrics, String root, long time, long resetTime) {
    String suffix;
    String serieSuffix;

    switch (metric) {
      case GETS:
        suffix = "-gets";
        serieSuffix = ".gets";
        break;
      case PUTS:
        suffix = "-puts";
        serieSuffix = ".puts";
        break;
      case DELETES:
        suffix = "-deletes";
        serieSuffix = ".deletes";
        break;
      case RANGES:
        suffix = "-ranges";
        serieSuffix = ".ranges";
        break;
      case BYTESREAD:
        suffix = "-bytes-read";
        serieSuffix = ".bytes.read";
        break;
      case BYTESWRITTEN:
        suffix = "-bytes-written";
        serieSuffix = ".bytes.written";
        break;
      default:
        throw new RuntimeException("Unknown metric: " + metric);

    }

    publishToInfluxDB(kvMetricLastValue, metrics, suffix, root, serieSuffix, time, resetTime);
  }

  private void publishCachedStoreMetric(KVStoreMetric metric, Map<String, Object> metrics, String root, long time, long resetTime) {
    String suffix;
    String serieSuffix;

    switch (metric) {
      case GETS:
        suffix = "-gets";
        serieSuffix = ".cached.gets";
        break;
      case PUTS:
        suffix = "-puts";
        serieSuffix = ".cached.puts";
        break;
      case DELETES:
        suffix = "-deletes";
        serieSuffix = ".cached.deletes";
        break;
      case RANGES:
        suffix = "-ranges";
        serieSuffix = ".cached.ranges";
        break;
      case BYTESREAD:
        suffix = "-bytes-read";
        serieSuffix = ".cached.bytes.read";
        break;
      case BYTESWRITTEN:
        suffix = "-bytes-written";
        serieSuffix = ".cached.bytes.written";
        break;
      default:
        throw new RuntimeException("Unknown metric: " + metric);

    }

    publishToInfluxDB(cachedMetricLastValue, metrics, suffix, root, serieSuffix, time, resetTime);
  }

  private void publishToInfluxDB(Map<String, Integer> metricLastValue, Map<String, Object> metrics, String suffix, String root, String serieSuffix, Long time, Long resetTime) {
    Set<String> keys = withSuffix(metrics.keySet(), suffix);
    BatchPoints batchPoints = BatchPoints
        .database(db)
        .tag("async", "true")
        .retentionPolicy("default")
        .consistency(InfluxDB.ConsistencyLevel.ALL)
        .build();
    for (String k : keys) {
      Integer metricVal = (Integer)metrics.get(k);
      Integer valForCurrentInterval = 0;
      if (metricVal != 0 && metricLastValue.containsKey(root + k)) {
        valForCurrentInterval = metricVal - metricLastValue.get(root + k);
      }

      metricLastValue.put(root + k, metricVal);

      Point p = Point.measurement(root + "." + k.substring(0, k.length() - suffix.length()) + serieSuffix)
          .time(time, TimeUnit.MILLISECONDS)
          .field("reset-time", resetTime)
          .field("value", valForCurrentInterval)
          .build();
      batchPoints.point(p);
    }

    influxDB.write(batchPoints);
  }

  private Set<String> withSuffix(Set<String> keys, String suffix) {
    Set<String> r = new HashSet<String>();
    for (String key : keys) {
      if (key.endsWith(suffix)) {
        r.add(key);
      }
    }

    return r;
  }
}

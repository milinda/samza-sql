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

import com.google.common.collect.Maps;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricsProcessorTask implements StreamTask, InitableTask {
  public static final String INFLUXDB_HOST = "samzasql.eval.influxdb.host";
  public static final String INFLUXDB_DB = "samzasql.eval.influxdb.db";

  private String influxDbHost;
  private InfluxDB influxDB;
  private String db;

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    influxDbHost = config.get(INFLUXDB_HOST, "localhost");
    db = config.get(INFLUXDB_DB, "samzasql-eval-" + System.currentTimeMillis());
    influxDB = InfluxDBFactory.connect("http://" + influxDbHost + ":8086", "root", "root");
    List<String> databases = influxDB.describeDatabases();

    if(!isDatabaseExists(db, databases)) {
      influxDB.createDatabase(db);
    }
  }

  private boolean isDatabaseExists(String dbName, List<String> databases) {
    for(String db : databases){
      if(db.equals(dbName)){
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

    if (metrics.containsKey("org.apache.samza.container.SamzaContainerMetrics")) {
      Map<String, Object> containerMetrics = (Map<String, Object>) metrics.get("org.apache.samza.container.SamzaContainerMetrics");

      int processEnvelops = (Integer) containerMetrics.get("process-envelopes");
      double taskProcessTime = (Double) containerMetrics.get("process-ms");
      String serieNameRoot = jobName + "." + jobId + "." + source;
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
          .field("envelops-processed", processEnvelops)
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
    }


  }
}

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

package org.apache.samza.sql.samples.system;

import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.*;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class OrdersStreamSystemFactory implements SystemFactory {

  private final AtomicInteger orderId = new AtomicInteger(0);
  private static final Integer[] productIds = new Integer[]{10, 34, 56, 78, 23, 34, 11, 15, 16, 18};
  private final Random rand = new Random(System.currentTimeMillis());
  private final AtomicInteger offset = new AtomicInteger(0);

  private static String loadOrdersSchema() throws IOException {
    return Resources.toString(OrdersStreamSystemFactory.class.getResource("/orders_avro.avsc"), Charset.defaultCharset());
  }
  @Override
  public SystemConsumer getConsumer(final String systemName, Config config, MetricsRegistry registry) {
    String ordersSchemaStr = null;
    try {
      ordersSchemaStr = loadOrdersSchema();
    } catch (IOException e) {
      throw new SamzaException("Cannot load orders schema.", e);
    }

    final Schema ordersSchema = new Schema.Parser().parse(ordersSchemaStr);

    final SystemStreamPartition streamPartition =
        new SystemStreamPartition(systemName, "orders", new Partition(0));
    return new BlockingEnvelopeMap() {
      @Override
      public void start() {
        while(true) {
          GenericRecordBuilder recordBuilder = new GenericRecordBuilder(ordersSchema);
          int productId = productIds[rand.nextInt(10)];
          recordBuilder.set("orderId", orderId.getAndIncrement());
          recordBuilder.set("productId", productId);
          recordBuilder.set("units", rand.nextInt(100));
          recordBuilder.set("rowtime", System.currentTimeMillis());
          try {
            put(streamPartition,
                new IncomingMessageEnvelope(streamPartition,
                    String.valueOf(offset.getAndIncrement()),
                    productId,
                    recordBuilder.build()));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          try {
            Thread.sleep(rand.nextInt(1000));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }

      @Override
      public void stop() {

      }
    };
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    throw new SamzaException("You can't produce to the order generation system");
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SinglePartitionWithoutOffsetsSystemAdmin();
  }
}

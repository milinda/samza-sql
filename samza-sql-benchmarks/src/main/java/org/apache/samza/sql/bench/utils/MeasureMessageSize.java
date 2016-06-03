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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.samza.SamzaException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MeasureMessageSize {
  private static RandomString r = new RandomString(85);
  private static  GenericRecord genProduct(Schema avroSchema) throws IOException {
    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(avroSchema);
    recordBuilder.set("orderId", 1003344030);
    recordBuilder.set("productId", 122330330);
    recordBuilder.set("units", 1000);
    recordBuilder.set("rowtime", System.currentTimeMillis());
    recordBuilder.set("padding", r.nextString());
    return recordBuilder.build();
  }

  public static void main(String[] args) throws IOException {
    Schema avroSchema = new Schema.Parser().parse(TestDataGenerator.loadOrdersSchema());
    final GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>(avroSchema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

    try {
      writer.write(genProduct(avroSchema), encoder);
      encoder.flush();
      System.out.println("Message size: " + out.toByteArray().length);
    } catch (IOException e) {
      String errMsg = "Cannot perform Avro binary encode.";
      throw new SamzaException(errMsg, e);
    }
  }
}

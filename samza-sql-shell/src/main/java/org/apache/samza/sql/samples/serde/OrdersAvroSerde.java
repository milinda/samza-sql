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

package org.apache.samza.sql.samples.serde;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.samza.SamzaException;
import org.apache.samza.serializers.Serde;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class OrdersAvroSerde implements Serde<Object> {
  private static final String ORDERS_SCHEMA = "{\n" +
      "  \"type\": \"record\",\n" +
      "  \"name\": \"orders_schema\",\n" +
      "  \"namespace\": \"org.apache.samza.sql.test\",\n" +
      "  \"fields\": [{\n" +
      "    \"name\": \"rowtime\",\n" +
      "    \"type\": \"long\"\n" +
      "  }, {\n" +
      "    \"name\": \"productId\",\n" +
      "    \"type\": \"int\"\n" +
      "  }, {\n" +
      "    \"name\": \"units\",\n" +
      "    \"type\": \"int\"\n" +
      "  }, {\n" +
      "    \"name\": \"orderId\",\n" +
      "    \"type\": \"int\"\n" +
      "  }]\n" +
      "}";

  private final Schema ordersSchema = new Schema.Parser().parse(ORDERS_SCHEMA);

  private final GenericDatumReader<GenericRecord> reader;
  private final GenericDatumWriter<Object> writer;

  public OrdersAvroSerde(){
    this.reader = new GenericDatumReader<GenericRecord>(ordersSchema);
    this.writer = new GenericDatumWriter<Object>(ordersSchema);
  }

  @Override
  public Object fromBytes(byte[] bytes) {
    try {
      return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
    } catch (IOException e) {
      throw new SamzaException("Cannot decode message.", e);
    }
  }

  @Override
  public byte[] toBytes(Object object) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

    try {
      writer.write(object, encoder);
      encoder.flush();
      return out.toByteArray();
    } catch (IOException e) {
      throw new SamzaException("Cannot encode avro data.", e);
    }
  }
}

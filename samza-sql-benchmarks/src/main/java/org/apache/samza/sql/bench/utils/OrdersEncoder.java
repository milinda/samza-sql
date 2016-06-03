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

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.samza.sql.bench.utils.TestDataGenerator.loadOrdersSchema;

public class OrdersEncoder implements Encoder<GenericRecord> {

  private final DatumWriter datumWriter;

  public OrdersEncoder(VerifiableProperties props) throws IOException {
    this.datumWriter = new GenericDatumWriter<GenericRecord>(new Schema.Parser().parse(loadOrdersSchema()));
  }

  @Override
  public byte[] toBytes(GenericRecord genericRecord) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, null);

    try {
      datumWriter.write(genericRecord, binaryEncoder);
    } catch (IOException e) {
      throw new RuntimeException("Cannot serialize Avro record!", e);
    }

    try {
      binaryEncoder.flush();
    } catch (IOException e) {
      try {
        out.close();
      } catch (IOException e1) {
        throw new RuntimeException("Cannot close output stream.");
      }

      throw new RuntimeException("Avro record serialization failed due to binary encoder flush error.", e);
    }

    try {
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("Cannot close output stream.");
    }

    return out.toByteArray();
  }
}


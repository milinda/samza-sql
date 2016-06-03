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

package org.apache.samza.sql.bench.serde;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.sql.bench.utils.DataVerifier;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ConfigurableAvroSerdeFactory  implements SerdeFactory<GenericRecord>{
  public static final String PROP_AVRO_SCHEMA = "serializers.%s.schema";

  @Override
  public Serde<GenericRecord> getSerde(String name, Config config) {
    String avroSchemaStr = config.get(String.format(PROP_AVRO_SCHEMA, name));
    if (avroSchemaStr == null || avroSchemaStr.isEmpty()) {
      throw new SamzaException("Cannot find avro schema for SerdeFactory '" + name + "'.");
    }

    final DataVerifier.SchemaType schemaType = DataVerifier.SchemaType.valueOf(avroSchemaStr.trim());
    try {
      final Schema schema = new Schema.Parser().parse(DataVerifier.loadSchema(schemaType));
      return new Serde<GenericRecord>() {
        private final GenericDatumReader<GenericRecord> reader  = new GenericDatumReader<GenericRecord>(schema);
        private final GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);

        @Override
        public GenericRecord fromBytes(byte[] bytes) {
          try{
            return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
          } catch (IOException e) {
            throw new SamzaException("Cannot decode record.", e);
          }
        }

        @Override
        public byte[] toBytes(GenericRecord object) {
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

          try {
            writer.write(object, encoder);
            encoder.flush();
            return out.toByteArray();
          } catch (IOException e) {
            String errMsg = "Cannot perform Avro binary encode.";
            throw new SamzaException(errMsg, e);
          }
        }
      };
    } catch (IOException e) {
      throw new SamzaException("Cannot load schema.", e);
    }
  }
}

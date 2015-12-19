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

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;

import java.io.*;

public class LongSerdeFactory implements SerdeFactory<Long> {
  @Override
  public Serde<Long> getSerde(String name, Config config) {
    return new Serde<Long>() {
      @Override
      public Long fromBytes(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        try{
          return dis.readLong();
        } catch (IOException e) {
          throw new SamzaException("Cannot deserialize integer.");
        }
      }

      @Override
      public byte[] toBytes(Long object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try {
          dos.writeLong(object);
          dos.flush();
        } catch (IOException e) {
          throw new SamzaException("Cannot serialize integer: " + object);
        }
        return bos.toByteArray();
      }
    };
  }
}

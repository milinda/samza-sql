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

package org.apache.samza.sql.bench.slidingwindow;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.sql.window.storage.TimeKey;

import java.io.*;

public class TimeKeySerdeFactory implements SerdeFactory<Object> {
  @Override
  public Serde<Object> getSerde(String name, Config config) {
    return new Serde<Object>() {
      @Override
      public Object fromBytes(byte[] bytes) {
        DataInputStream ois = null;
        try {
          ois = new DataInputStream(new ByteArrayInputStream(bytes));
          return new TimeKey(ois.readLong());
        } catch (Exception e) {
          throw new SamzaException("Cannot deserialize byte array.", e);
        }
      }

      @Override
      public byte[] toBytes(Object object) {
        TimeKey toKey = (TimeKey) object;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          DataOutputStream oos = new DataOutputStream(baos);
          oos.writeLong(toKey.getTimeNano());
          oos.close();
          return baos.toByteArray();
        } catch (Exception e) {
          throw new SamzaException("Cannot serialize object.", e);
        }
      }
    };
  }
}

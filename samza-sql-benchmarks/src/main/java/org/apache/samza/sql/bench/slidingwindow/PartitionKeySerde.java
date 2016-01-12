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
import org.apache.samza.serializers.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class PartitionKeySerde implements Serde<Object> {
  private static final Logger log = LoggerFactory.getLogger(PartitionKeySerde.class);

  @Override
  public Object fromBytes(byte[] bytes) {
    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
      return new SlidingWindowSumOperatorFactory.PartitionKey((Object[])ois.readObject());
    } catch (Exception e) {
      log.error("Cannot deserialize byte array.", e);
      throw new SamzaException("Cannot deserialize byte array.", e);
    }
  }

  @Override
  public byte[] toBytes(Object object) {
    SlidingWindowSumOperatorFactory.PartitionKey pkey = (SlidingWindowSumOperatorFactory.PartitionKey)object;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(pkey.getValues());
      oos.close();
      return baos.toByteArray();
    } catch (Exception e) {
      log.error("Cannot serialize object.", e);
      throw new SamzaException("Cannot serialize object.", e);
    }
  }
}

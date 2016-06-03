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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.window.storage.TimeAndOffsetKey;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class TimeAndOffsetKeySerdeFactory implements SerdeFactory<Object> {
  private final Kryo kryo = new Kryo();

  @Override
  public Serde<Object> getSerde(String name, Config config) {
    return new Serde<Object>() {
      @Override
      public Object fromBytes(byte[] bytes) {
        Input input = new Input(new ByteArrayInputStream(bytes));
        TimeAndOffsetKey tuple = kryo.readObject(input, TimeAndOffsetKey.class);
        input.close();

        return tuple;
      }

      @Override
      public byte[] toBytes(Object object) {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        Output output = new Output(bao);
        kryo.writeObject(output, (TimeAndOffsetKey)object);
        output.close();

        return bao.toByteArray();
      }
    };
  }
}

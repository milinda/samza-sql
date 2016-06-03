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
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.sql.physical.window.TimeBasedSlidingWindowAggregatorState;
import org.apache.samza.sql.window.storage.TimeAndOffsetKey;
import org.apache.samza.system.sql.LongOffset;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

public class TimeBasedSlidingWindowAggregatorStateSerdeFactory implements SerdeFactory<Object> {
  private static final Kryo kryo = new Kryo();

  static {
//    kryo.register(TimeAndOffsetKey.class, new Serializer<TimeAndOffsetKey>() {
//      @Override
//      public void write(Kryo kryo, Output output, TimeAndOffsetKey object) {
//        output.writeLong(object.getTimeNano());
//        output.writeLong(((LongOffset)object.getOffset()).getOffset());
//      }
//
//      @Override
//      public TimeAndOffsetKey read(Kryo kryo, Input input, Class type) {
//        TimeAndOffsetKey o = new TimeAndOffsetKey();
//        o.setTimeNano(input.readLong());
//        o.setOffset(new LongOffset(input.readLong()));
//        return o;
//      }
//    });
//    CollectionSerializer serializer = new CollectionSerializer();
    kryo.register(TimeAndOffsetKey.class);
    kryo.register(LongOffset.class);
    kryo.register(TimeBasedSlidingWindowAggregatorState.class);
    kryo.register(ArrayList.class);
  }

  @Override
  public Serde<Object> getSerde(String name, Config config) {
    return new Serde<Object>() {
      @Override
      public Object fromBytes(byte[] bytes) {
        Input input = new Input(new ByteArrayInputStream(bytes));
        TimeBasedSlidingWindowAggregatorState s = kryo.readObject(input, TimeBasedSlidingWindowAggregatorState.class);

        if(s != null) {
          ArrayList<TimeAndOffsetKey> tuples = s.getTuples();
        } else {
          return null;
        }
        return s;
      }

      @Override
      public byte[] toBytes(Object object) {
        TimeBasedSlidingWindowAggregatorState s = (TimeBasedSlidingWindowAggregatorState) object;
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        Output output = new Output(bao);
        kryo.writeObject(output, s);
        output.close();

        return bao.toByteArray();
      }
    };
  }
}

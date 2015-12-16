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
import org.apache.samza.serializers.Serde;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.numbers.IntegerData;
import org.apache.samza.system.sql.LongOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;

public class IntermediateMessageTupleSerde implements Serde<Object> {
  private static final Logger log = LoggerFactory.getLogger(IntermediateMessageTuple.class);

  private final Kryo kryo = new Kryo();

  @Override
  public Object fromBytes(byte[] bytes) {
    Input input = new Input(new ByteArrayInputStream(bytes));
    IntermediateMessageTuple tuple = kryo.readObject(input, IntermediateMessageTuple.class);
    input.close();

    return tuple;
  }

  @Override
  public byte[] toBytes(Object object) {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    Output output = new Output(bao);
    kryo.writeObject(output, (IntermediateMessageTuple)object);
    output.close();

    return bao.toByteArray();
  }

  public static void main(String[] args) {
    IntermediateMessageTuple it = new IntermediateMessageTuple(
        new Object[]{1, "hello", 5.5f},
        new IntegerData(2),
        System.currentTimeMillis(),
        new LongOffset("1"),
        false,
        EntityName.getStreamName("kafka:orders"));

    IntermediateMessageTupleSerde serde = new IntermediateMessageTupleSerde();

    byte[] b = serde.toBytes(it);
    IntermediateMessageTuple ito = (IntermediateMessageTuple) serde.fromBytes(b);
    System.out.println(Arrays.toString(ito.getContent()));
    System.out.println(ito.getEntityName());
    System.out.println(ito.getKey());
    System.out.println(ito.getOffset());
    System.out.println(ito.isDelete());
  }
}

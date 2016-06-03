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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.samza.serializers.Serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class ObjectArraySerde implements Serde<Object[]> {
  private final Kryo kryo = new Kryo();

  @Override
  public Object[] fromBytes(byte[] bytes) {
    Input input = new Input(new ByteArrayInputStream(bytes));
    Object[] out = kryo.readObject(input, Object[].class);
    input.close();

    return out;
  }

  @Override
  public byte[] toBytes(Object[] object) {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    Output output = new Output(bao);
    kryo.writeObject(output, object);
    output.close();

    return bao.toByteArray();
  }
}

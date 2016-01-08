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
package org.apache.samza.sql.data;

import org.apache.samza.sql.api.data.Data;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.system.sql.Offset;

import java.io.Serializable;

public class IntermediateMessageTuple implements Tuple, Serializable {

  public enum Operation {
    INSERT,
    UPDATE,
    DELETE;
  }

  /**
   * Tuple content, converted to Object array format understood by Calcite
   */
  private Object[] message;

  // TODO: This should be receive time
  private long creationTime;

  private Offset offset;

  private EntityName streamName;

  private Data key;

  private boolean delete;

  private Operation operation;

  public IntermediateMessageTuple(){}

  public IntermediateMessageTuple(Object[] message, Data key, long creationTime, Offset offset,
                                  boolean delete, EntityName streamName) {
    this(message, key, creationTime, offset, delete, streamName, Operation.INSERT);
  }

  public IntermediateMessageTuple(Object[] message, Data key, long creationTime, Offset offset,
                                  boolean delete, EntityName streamName, Operation operation) {
    this.message = message;
    this.creationTime = creationTime;
    this.offset = offset;
    this.streamName = streamName;
    this.key = key;
    this.delete = delete;
    this.operation = operation;
  }

  public void setMessage(Object[] message) {
    this.message = message;
  }

  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }

  public void setOffset(Offset offset) {
    this.offset = offset;
  }

  public void setStreamName(EntityName streamName) {
    this.streamName = streamName;
  }

  public void setKey(Data key) {
    this.key = key;
  }

  public void setDelete(boolean delete) {
    this.delete = delete;
  }

  /**
   * Gets the Object array representation of the tuple.
   * @return tuple represented as a object array.
   */
  public Object[] getContent() {
    return message;
  }

  @Override
  public Data getMessage() {
    throw new UnsupportedOperationException("Not allowed. Please use getTuple.");
  }

  @Override
  public boolean isDelete() {
    return delete;
  }

  @Override
  public Data getKey() {
    return key;
  }

  @Override
  public EntityName getEntityName() {
    return streamName;
  }

  @Override
  public long getCreateTimeNano() {
    return creationTime;
  }

  public long getCreationTimeMillis() {
    return creationTime / 1000000;
  }

  @Override
  public Offset getOffset() {
    return offset;
  }

  public Operation getOperation() {
    return operation;
  }

  public static IntermediateMessageTuple fromData(Object[] tuple, Data key, long creationTime,
                                                  Offset offset, boolean delete,
                                                  EntityName streamName) {
    return new IntermediateMessageTuple(tuple, key, creationTime, offset, delete, streamName);
  }

  public static IntermediateMessageTuple fromTuple(IntermediateMessageTuple tuple, EntityName streamName) {
    return new IntermediateMessageTuple(tuple.message, tuple.key, tuple.creationTime, tuple.offset, tuple.delete, streamName);
  }

  public static IntermediateMessageTuple fromTupleAndContent(IntermediateMessageTuple tuple, Object[] content, EntityName streamName) {
    return new IntermediateMessageTuple(content, tuple.key, tuple.creationTime, tuple.offset, tuple.delete, streamName);
  }

  public static IntermediateMessageTuple fromRelationChangeLog(Object[] tuple, Data key, long creationTime,
                                                               Offset offset, boolean delete,
                                                               EntityName streamName, Operation operation) {
    return new IntermediateMessageTuple(tuple, key, creationTime, offset, delete, streamName, operation);
  }
}

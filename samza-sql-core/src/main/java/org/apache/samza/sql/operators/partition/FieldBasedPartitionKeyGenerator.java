/**
 * Copyright (C) 2015 Trustees of Indiana University
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.samza.sql.operators.partition;

import org.apache.samza.sql.api.data.Data;
import org.apache.samza.sql.api.data.Schema;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.expressions.ScalarExpression;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Simple partition key generator based on tuple fields. This generator concatenate string
 * representation of field values to a single string to generate the partition key. Each partition
 * by field value should be of primitive type (INT, LONG, BOOLEAN, FLOAT, DOUBLE, STRING).
 */
public class FieldBasedPartitionKeyGenerator implements ScalarExpression {

  private final Deque<String> partitionByFields;

  public FieldBasedPartitionKeyGenerator(String... fields) {
    partitionByFields = new ArrayDeque<String>();

    for (String field : fields) {
      partitionByFields.offer(field);
    }
  }

  @Override
  public Object execute(Object[] inputValues) {
    throw new UnsupportedOperationException("Partition key generator doesn't support this method.");
  }

  @Override
  public Object execute(Tuple tuple) {
    Data message = tuple.getMessage();
    Schema.Type messageType = message.schema().getType();

    if(messageType == Schema.Type.STRUCT) {
      throw new IllegalArgumentException(String.format("Unsupported type: %s", messageType));
    }

    StringBuilder sb = new StringBuilder();

    for(String field : partitionByFields) {
      Data fieldData = message.getFieldData(field);
      Schema.Type fieldType = fieldData.schema().getType();

      if(!fieldType.isPrimitive()){
        throw new IllegalArgumentException(String.format("Unsupported partition by field type: %s",
            fieldType));
      }

      sb.append(String.valueOf(fieldData.value()));
    }

    return sb.toString();
  }
}

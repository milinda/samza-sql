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

package org.apache.samza.sql.numbers;

import org.apache.samza.sql.api.data.Data;
import org.apache.samza.sql.api.data.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class IntegerData implements Data, Serializable {

  private static final long serialVersionUID = -1658598299151099582L;

  private  Integer datum;
  private  Schema schema;

  public IntegerData(){}

  public IntegerData(Object datum) {
    this.datum = (Integer) datum;
    this.schema = new IntegerSchema();
  }

  public Integer getDatum() {
    return datum;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setDatum(Integer datum) {
    this.datum = datum;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  @Override
  public Schema schema() {
    return this.schema;
  }

  @Override
  public Object value() {
    return this.datum;
  }

  @Override
  public int intValue() {
    return datum;
  }

  @Override
  public long longValue() {
    throw new UnsupportedOperationException("Can't get long value for a int type data");
  }

  @Override
  public float floatValue() {
    throw new UnsupportedOperationException("Can't get float value for a int type data");
  }

  @Override
  public double doubleValue() {
    throw new UnsupportedOperationException("Can't get double value for a int type data");
  }

  @Override
  public boolean booleanValue() {
    throw new UnsupportedOperationException("Can't get boolean value for a int type data");
  }

  @Override
  public String strValue() {
    throw new UnsupportedOperationException("Can't get string value for a int type data");
  }

  @Override
  public byte[] bytesValue() {
    throw new UnsupportedOperationException("Can't get bytesValue for a int type data");
  }

  @Override
  public List<Object> arrayValue() {
    throw new UnsupportedOperationException("Can't get arrayValue for a int type data");
  }

  @Override
  public Map<Object, Object> mapValue() {
    throw new UnsupportedOperationException("Can't get mapValue for a int type data");
  }

  @Override
  public Data getElement(int index) {
    throw new UnsupportedOperationException("Can't getElement(index) on a int type data");
  }

  @Override
  public Data getFieldData(String fldName) {
    throw new UnsupportedOperationException("Can't getFieldData(fieldName) for a int type data");
  }

  @Override
  public String toString() {
    return String.valueOf(datum);
  }
}

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
package org.apache.samza.sql.operators.window2;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.expressions.ScalarExpression;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.operators.SimpleOperatorSpec;
import org.apache.samza.sql.operators.window.RetentionPolicy;

import java.util.HashMap;
import java.util.Map;

public class WindowSpec extends SimpleOperatorSpec implements OperatorSpec {

  public enum SizeUnit {
    TIME_SEC,
    TIME_MS,
    TIME_MICRO,
    TIME_NANO,
    TUPLE
  }

  public enum Type {
    FIXED_WND,           // Hopping, tumbling and sliding windows
    SESSION_WND          // Window with dynamic size
  }

  private final SizeUnit sizeUnit;

  private final Type type;

  private final RetentionPolicy retentionPolicy;

  private final String timestampField;


  /**
   * Named list of aggregate calls
   */
  private final Map<String, ScalarExpression> aggregates = new HashMap<String, ScalarExpression>();

  // TODO: join spec

  protected WindowSpec(String id, EntityName input, EntityName output, SizeUnit sizeUnit, Type type,
                     RetentionPolicy retentionPolicy, String timestampField) {
    super(id, input, output);
    this.sizeUnit = sizeUnit;
    this.type = type;
    this.retentionPolicy = retentionPolicy;
    this.timestampField = timestampField;
  }

  protected WindowSpec(String id, EntityName input, EntityName output, SizeUnit sizeUnit, Type type,
                       RetentionPolicy retentionPolicy, String timestampField, Map<String, ScalarExpression> aggregates) {
    super(id, input, output);
    this.sizeUnit = sizeUnit;
    this.type = type;
    this.retentionPolicy = retentionPolicy;
    this.aggregates.putAll(aggregates);
    this.timestampField = timestampField;
  }

  public SizeUnit getSizeUnit() {
    return sizeUnit;
  }

  public Type getType() {
    return type;
  }

  public RetentionPolicy getRetentionPolicy() {
    return retentionPolicy;
  }

  public boolean isAggregate() {
    return aggregates.size() > 0;
  }

  public boolean isJoin() {
    // TODO: Fix this.
    return false;
  }

  public Map<String, ScalarExpression> getAggregates() {
    return aggregates;
  }

  public String getTimestampField() {
    return timestampField;
  }

  /**
   * Helper function to convert a long value to time in nano seconds based on the time unit specified in this class
   *
   * @param longValue The long value to be converted
   * @return The corresponding time in nano second
   */
  public long getNanoTime(long longValue) {
    switch (this.sizeUnit) {
    case TIME_SEC:
      return longValue * 1000000000;
    case TIME_MS:
      return longValue * 1000000;
    case TIME_MICRO:
      return longValue * 1000;
    case TIME_NANO:
    default:
      return longValue;
    }
  }
}

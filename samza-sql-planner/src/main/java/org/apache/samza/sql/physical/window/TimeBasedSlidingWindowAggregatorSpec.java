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

package org.apache.samza.sql.physical.window;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexLiteral;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.operators.SimpleOperatorSpec;

import java.security.Timestamp;
import java.util.List;

public class TimeBasedSlidingWindowAggregatorSpec extends SimpleOperatorSpec {

  public enum TimeUnit {
    TIME_HOUR,
    TIME_MIN,
    TIME_SEC,
    TIME_MS,
    TIME_MICRO,
    TIME_NANO
  }

  private final List<Window.Group> groups;

  private final ImmutableList<RexLiteral> constants;

  public TimeBasedSlidingWindowAggregatorSpec(String id, EntityName input, EntityName output,
                                              List<Window.Group> groups, ImmutableList<RexLiteral> constants) {
    super(id, input, output);
    this.groups = groups;
    this.constants = constants;
  }

  public List<Window.Group> getGroups() {
    return groups;
  }
}

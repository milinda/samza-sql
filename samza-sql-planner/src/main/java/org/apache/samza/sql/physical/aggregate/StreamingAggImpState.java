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

package org.apache.samza.sql.physical.aggregate;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.core.AggregateCall;

import java.util.List;

public class StreamingAggImpState {
  public final int aggIdx;
  public final AggregateCall call;
  public final StreamingAggImplementor implementor;
  public StreamingAggContext context;
  public Expression result;
  public List<Expression> state;

  public StreamingAggImpState(int aggIdx, AggregateCall call, boolean windowContext) {
    this.aggIdx = aggIdx;
    this.call = call;
    this.implementor =
        StreamingRexImplTable.INSTANCE.get(call.getAggregation());
    if (implementor == null) {
      throw new IllegalArgumentException(
          "Unable to get aggregate implementation for aggregate "
              + call.getAggregation()
              + (windowContext ? " in window context" : ""));
    }
  }
}

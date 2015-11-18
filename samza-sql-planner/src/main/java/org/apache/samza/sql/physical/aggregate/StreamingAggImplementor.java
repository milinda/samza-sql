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


import org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.calcite.linq4j.tree.Expression;

import java.util.List;

public interface StreamingAggImplementor extends AggImplementor {

  /**
   * Updates intermediate values to account for the value removed from the window.
   * {@link StreamingAggSubtractContext#accumulator()} should be used to reference
   * the state variables.
   *
   * @param info Streaming aggregate context
   * @param sub  Streaming aggregate subtract context
   */
  Expression implementSubtract(StreamingAggContext info, StreamingAggSubtractContext sub);

  Expression implementStreamingAdd(StreamingAggContext info, StreamingAggAddContext add);

}

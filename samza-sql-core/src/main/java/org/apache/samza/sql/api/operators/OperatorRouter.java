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

package org.apache.samza.sql.api.operators;

import org.apache.samza.sql.api.data.EntityName;

import java.util.Iterator;
import java.util.List;


/**
 * This interface class defines interface methods to connect operators together.
 *
 * <p>The {@code OperatorRouter} allows the user to attach operators to a relation or a stream entity,
 * if the corresponding relation/stream is included as inputs to the operator. Each operator then executes its own logic
 * and determines which relation/stream to emit the output to. Through the {@code OperatorRouter}, the next
 * operators attached to the corresponding output entities (i.e. tables/streams) can then be invoked to continue the
 * stream process task.
 */
public interface OperatorRouter extends Operator {

  /**
   * This method adds a {@link org.apache.samza.sql.api.operators.SimpleOperator}.
   *
   * @param nextOp The {@link org.apache.samza.sql.api.operators.SimpleOperator}.
   * @throws Exception Throws exception if failed
   */
  void addOperator(SimpleOperator nextOp) throws Exception;

  /**
   * This method gets the list of {@link org.apache.samza.sql.api.operators.SimpleOperator}s attached to an output entity (of any type)
   *
   * @param output The identifier of the output entity
   * @return The list of {@link org.apache.samza.sql.api.operators.SimpleOperator} taking {@code output} as input variables
   */
  List<SimpleOperator> getNextOperators(EntityName output);

  /**
   * Method to return an iterator of all operators in the router
   *
   * @return The {@link java.util.Iterator} for all {@link org.apache.samza.sql.api.operators.SimpleOperator}s
   */
  Iterator<SimpleOperator> iterator();

}

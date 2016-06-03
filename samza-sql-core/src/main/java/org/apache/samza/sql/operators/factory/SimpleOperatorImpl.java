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

package org.apache.samza.sql.operators.factory;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.api.operators.SimpleOperator;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;


/**
 * An abstract class that encapsulate the basic information and methods that all operator classes should implement.
 *
 */
public abstract class SimpleOperatorImpl implements SimpleOperator {
  /**
   * The specification of this operator
   */
  private final OperatorSpec spec;

  /**
   * User specified callback object
   */
  protected final OperatorCallback opCallback;

  /**
   * Ctor of <code>SimpleOperator</code> class
   *
   * @param spec The specification of this operator
   */
  public SimpleOperatorImpl(OperatorSpec spec) {
    this.spec = spec;
    this.opCallback = NoopOperatorCallback.getInstance();
  }

  public SimpleOperatorImpl(OperatorSpec spec, OperatorCallback callback) {
    this.spec = spec;
    this.opCallback = callback;
  }

  @Override
  public OperatorSpec getSpec() {
    return this.spec;
  }

  @Override
  public void process(Relation deltaRelation, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    Relation rel = this.opCallback.beforeProcess(deltaRelation, collector, coordinator);
    MessageCollector opCollector = collector;
    if (!(opCollector instanceof SimpleMessageCollector)) {
      opCollector = new SimpleMessageCollector(opCollector, coordinator);
    }
    if (rel == null) {
      return;
    }
    this.realProcess(rel, opCollector, coordinator);
  }

  @Override
  public void process(Tuple tuple, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    Tuple ituple = this.opCallback.beforeProcess(tuple, collector, coordinator);
    MessageCollector opCollector = collector;
    if (!(opCollector instanceof SimpleMessageCollector)) {
      opCollector = new SimpleMessageCollector(opCollector, coordinator);
    }
    if (ituple == null) {
      return;
    }
    this.realProcess(ituple, opCollector, coordinator);
  }

  protected abstract void realProcess(Relation rel, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception;

  protected abstract void realProcess(Tuple ituple, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception;
}

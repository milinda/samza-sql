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
package org.apache.samza.sql.physical.project;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.operators.SimpleOperatorImpl;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

public class Project extends SimpleOperatorImpl {
  private final ProjectSpec spec;

  private final RelDataType type;

  public Project(ProjectSpec spec, RelDataType type) {
    super(spec);
    this.spec = spec;
    this.type = type;
  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector,
                             TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Relation rel, SimpleMessageCollector collector,
                             TaskCoordinator coordinator) throws Exception {
    // TODO: Implement relation project
  }

  @Override
  protected void realProcess(Tuple ituple, SimpleMessageCollector collector,
                             TaskCoordinator coordinator) throws Exception {
    if(!(ituple instanceof IntermediateMessageTuple)) {
      throw new SamzaException("Only tuples of type IntermediateMessageTuple supported at this stage.");
    }

    IntermediateMessageTuple t = (IntermediateMessageTuple)ituple;
    Object[] output = new Object[type.getFieldCount()];
    spec.getProject().execute(t.getContent(), output);

    collector.send(IntermediateMessageTuple.fromData(output, ituple.getKey(),
        ituple.getCreateTimeNano(), ituple.getOffset(), ituple.isDelete(), spec.getOutputName()));
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {

  }
}

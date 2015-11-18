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
package org.apache.samza.sql.operators.scan;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.operators.SimpleOperatorImpl;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

/**
 * This is a built-in operator that performs table scan.
 */
public class TableScan extends SimpleOperatorImpl {

  /**
   * The specification of this {@code TableScan}
   */
  private final TableScanSpec spec;

  /**
   * Ctor that takes {@link org.apache.samza.sql.operators.scan.TableScanSpec} object as input
   *
   * @param spec The <code>TableScanSpec</code> object
   */
  public TableScan(TableScanSpec spec) {
    super(spec);
    this.spec = spec;
  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  protected void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  protected void realProcess(Tuple ituple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // TODO Auto-generated method stub
  }
}

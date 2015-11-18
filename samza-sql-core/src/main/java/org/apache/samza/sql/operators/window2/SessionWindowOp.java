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

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.sql.window.storage.WindowState;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

import java.util.List;

public class SessionWindowOp extends WindowOp implements Operator {

  private final SessionWindowSpec spec;

  public SessionWindowOp(SessionWindowSpec spec, OperatorCallback callback) {
    super(spec, callback);
    this.spec = spec;
  }

  public SessionWindowOp(SessionWindowSpec spec) {
    super(spec);
    this.spec = spec;
  }

  @Override
  protected boolean isExpired(WindowState window) {
    return false;
  }

  @Override
  protected boolean isExpired(Tuple tuple) {
    return false;
  }

  @Override
  protected boolean isReplay(Tuple tuple) {
    return false;
  }

  @Override
  protected List<OrderedStoreKey> getWindows(Tuple tuple) {
    return null;
  }

  @Override
  protected void flush() throws Exception {

  }

  @Override
  protected void refresh() throws Exception {

  }

  @Override
  protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  protected void realProcess(Tuple ituple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }
}

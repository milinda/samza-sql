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

package org.apache.samza.sql.operators.window;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.window.storage.MessageStore;
import org.apache.samza.task.TaskContext;


/**
 * This abstract class defines the base class for all window operators that include a full {@code messageStore}
 */
public abstract class FullStateWindowOp extends WindowOp {

  protected MessageStore messageStore;

  FullStateWindowOp(WindowOpSpec spec, OperatorCallback callback) {
    super(spec, callback);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    super.init(config, context);
    this.messageStore = (MessageStore) context.getStore("wnd-msg-" + this.wndId);
  }

}

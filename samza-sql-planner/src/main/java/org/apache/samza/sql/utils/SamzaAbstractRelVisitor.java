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
package org.apache.samza.sql.utils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitDispatcher;
import org.apache.calcite.util.ReflectiveVisitor;

public abstract class SamzaAbstractRelVisitor extends RelVisitor implements ReflectiveVisitor {
  private final ReflectiveVisitDispatcher<SamzaAbstractRelVisitor, RelNode> dispatcher =
      ReflectUtil.createDispatcher(SamzaAbstractRelVisitor.class, RelNode.class);

  @Override
  public void visit(RelNode node, int ordinal, RelNode parent) {
    node.childrenAccept(this);
    dispatcher.invokeVisitor(this, node, "visit");
  }

  /**
   * Fallback visit method.
   * @param r RelNode instance to visit
   */
  public void visit(RelNode r){

  }
}

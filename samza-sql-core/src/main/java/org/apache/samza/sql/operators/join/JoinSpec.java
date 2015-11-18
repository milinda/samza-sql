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
package org.apache.samza.sql.operators.join;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.expressions.ScalarExpression;
import org.apache.samza.sql.operators.SimpleOperatorSpec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JoinSpec extends SimpleOperatorSpec {

  private ScalarExpression joinCondition;

  private JoinType joinType;

  private EntityName left;

  private EntityName right;

  public JoinSpec(String id, EntityName left, EntityName right, EntityName output, JoinType joinType,
                  ScalarExpression joinCondition) {
    super(id, makeList(left, right), output);
    this.left = left;
    this.right = right;
    this.joinType = joinType;
    this.joinCondition = joinCondition;
  }

  public ScalarExpression getJoinCondition() {
    return joinCondition;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public EntityName getLeft() {
    return left;
  }

  public EntityName getRight() {
    return right;
  }

  private static List<EntityName> makeList(EntityName... entityNames) {
    List<EntityName> entityNameList = new ArrayList<EntityName>();

    entityNameList.addAll(Arrays.asList(entityNames));

    return entityNameList;
  }
}

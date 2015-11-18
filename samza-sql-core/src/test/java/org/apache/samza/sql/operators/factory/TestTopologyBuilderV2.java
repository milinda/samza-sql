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
package org.apache.samza.sql.operators.factory;

import junit.framework.Assert;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.expressions.ScalarExpression;
import org.apache.samza.sql.api.expressions.TupleExpression;
import org.apache.samza.sql.api.operators.OperatorRouter;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.api.operators.SimpleOperator;
import org.apache.samza.sql.operators.SimpleRouter;
import org.apache.samza.sql.operators.join.JoinType;
import org.apache.samza.sql.operators.modify.Operation;
import org.apache.samza.sql.operators.project.ProjectOp;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class TestTopologyBuilderV2 {

  @Test
  public void testFilterProjectTopology() {
    TopologyBuilderV2 builder = TopologyBuilderV2.create();

    OperatorRouter router = builder.scan(EntityName.getStreamName("kafka:page_view_stream"))
        .scan(EntityName.getTableName("espresso:page_group_key"))
        .join(JoinType.INNER, new NaturalJoinExpression())
        .modify(Operation.INSERT, EntityName.getStreamName("kafka:page_group_stream"))
        .buildQuery();

    Assert.assertNotNull(router);
  }

  @Test
  public void testViewTopology() {
    TopologyBuilderV2 builder = TopologyBuilderV2.create();

    TopologyBuilderV2.View filterStream = builder.scan(EntityName.getStreamName("kafka:orders"))
        .filter(new ScalarExpression() {
          @Override
          public Object execute(Object[] inputValues) {
            return null;
          }

          @Override
          public Object execute(Tuple tuple) {
            return null;
          }
        })
        .buildView();

    OperatorRouter router = builder.scan(filterStream)
        .project(new TupleExpression() {
          @Override
          public void execute(Object[] inputValues, Object[] results) {

          }

          @Override
          public Tuple execute(Tuple tuple) {
            return null;
          }
        })
        .buildQuery();

    // Count number of operators in final router
    int n = 0;
    Iterator<SimpleOperator> operators = router.iterator();

    while (operators.hasNext()) {
      operators.next();
      n++;
    }

    Assert.assertEquals(3, n);

    // Validate whether the topology is correct
    OperatorSpec filterSpecFromView = filterStream.lastOperatorSpec();
    List<SimpleOperator> nextOps =
        router.getNextOperators(filterSpecFromView.getOutputNames().get(0));

    Assert.assertEquals(nextOps.size(), 1);
    Assert.assertTrue(nextOps.get(0) instanceof ProjectOp);
  }


  public static class NaturalJoinExpression implements ScalarExpression {

    @Override
    public Object execute(Object[] inputValues) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object execute(Tuple tuple) {
      // TODO Auto-generated method stub
      return null;
    }
  }
}

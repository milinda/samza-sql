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
package org.apache.samza.sql.operators.aggregate;

import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.expressions.ScalarExpression;

public class Count implements AggregateFunction<Tuple> {

  /**
   * Expression to check whether field we are counting exists.
   */
  private final ScalarExpression countCheck;

  public Count(ScalarExpression countCheck) {
    this.countCheck = countCheck;
  }

  @Override
  public Aggregate lift(Tuple input) {

    if((Boolean)countCheck.execute(input)) {
      return new CountAggregate(1);
    }

    return new CountAggregate(0);
  }

  @Override
  public Aggregate combine(Aggregate a, Aggregate b) {
    return new CountAggregate(((CountAggregate)a).getCount() + ((CountAggregate)b).getCount());
  }

  @Override
  public Output lower(Aggregate partial) {
    return new Output<Integer>(((CountAggregate)partial).getCount());
  }

  public static class CountAggregate implements Aggregate {
    private int count;

    public CountAggregate(int count){
      this.count = count;
    }

    public int getCount() {
      return count;
    }
  }
}

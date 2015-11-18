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
package org.apache.samza.sql.operators.window2.aggregate;

import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.expressions.ScalarExpression;
import org.apache.samza.sql.operators.aggregate.AggregatorList;
import org.apache.samza.sql.operators.aggregate.GroupKey;

import java.util.HashMap;
import java.util.Map;

public class Grouping {

  private static final String NO_GROUPING_KEY = "NO_GROUPING";

  // Fields from 'GROUP BY'/'PARTITION BY' clause
  private final String[] fields;

  // Aggregation without grouping (e.g. SELECT STREAM COUNT(*) OVER (ORDER BY rowtime RANGE INTERVAL '10' MINUTE PRECEDING) FROM Orders)
  private boolean noGrouping = false;

  private final Map<GroupKey, AggregatorList> aggregates = new HashMap<GroupKey, AggregatorList>();

  private final Map<String, ScalarExpression> namedAggregateExpressions;

  public Grouping(String[] fields, Map<String, ScalarExpression> namedAggregateExpressions) {
    if (fields.length == 0) {
      noGrouping = true;
      this.fields = new String[]{};
    } else {
      this.fields = fields;
    }

    this.namedAggregateExpressions = namedAggregateExpressions;
  }

  public void send(Tuple t) {
    GroupKey key;
    if (noGrouping) {
      key = GroupKey.of(NO_GROUPING_KEY);
    } else {
      Object[] groupKeyValues = new Object[fields.length];

      for(int i = 0; i < fields.length; i++) {
        groupKeyValues[i] = t.getMessage().getFieldData(fields[i]).value();
      }

      key = GroupKey.of(groupKeyValues);
    }

    if(!aggregates.containsKey(key)){
      AggregatorList aggregatorList = new AggregatorList();

    }

  }

  public Map<GroupKey, AggregatorList> getAggregates() {
    return aggregates;
  }
}

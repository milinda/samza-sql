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

/**
 * General contract of a aggregate function. Inspired by paper 'General incremental sliding
 * window aggregation'.
 */
public interface AggregateFunction<T> {

  /**
   * Computes the partial aggregation for a single tuple.
   *
   * @param input The {@link org.apache.samza.sql.api.data.Tuple} instance for aggregation
   * @return Partial {@link org.apache.samza.sql.operators.aggregate.Aggregate} instance
   */
  Aggregate lift(T input);

  /**
   * Transforms partial aggregations for two sub-windows into a partial aggregation of the combined
   * sub-window
   *
   * @param a The partial {@link org.apache.samza.sql.operators.aggregate.Aggregate} instance
   * @param b The partial {@link org.apache.samza.sql.operators.aggregate.Aggregate} instance
   * @return Partial {@link org.apache.samza.sql.operators.aggregate.Aggregate} instance of combined
   * window
   */
  Aggregate combine(Aggregate a, Aggregate b);

  /**
   * Turns a partial aggregation for the entire window into an output
   *
   * @param partial The partial {@link org.apache.samza.sql.operators.aggregate.Aggregate} instance
   * @return Aggreation {@link org.apache.samza.sql.operators.aggregate.Output} instance
   */
  Output lower(Aggregate partial);

}


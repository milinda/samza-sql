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
package org.apache.samza.sql.operators.project;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.expressions.ScalarExpression;
import org.apache.samza.sql.api.expressions.TupleExpression;
import org.apache.samza.sql.operators.SimpleOperatorSpec;

public class ProjectSpec extends SimpleOperatorSpec {

  private TupleExpression project;

  public ProjectSpec(String id, EntityName input, EntityName output, TupleExpression project) {
    super(id, input, output);
    this.project = project;
  }

  public TupleExpression getProject() {
    return project;
  }
}

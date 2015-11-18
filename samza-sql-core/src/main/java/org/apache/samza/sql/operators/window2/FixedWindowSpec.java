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

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.operators.window.RetentionPolicy;

public class FixedWindowSpec extends WindowSpec implements OperatorSpec {

  private final int size;

  private final int hopSize;


  private FixedWindowSpec(String id, EntityName input, EntityName output, int size, int hopSize,
                          String timestampBy, SizeUnit sizeUnit, RetentionPolicy retentionPolicy) {
    super(id, input, output, sizeUnit, Type.FIXED_WND, retentionPolicy, timestampBy);
    this.size = size;
    this.hopSize = hopSize;
  }

  public int getSize() {
    return size;
  }

  public int getHopSize() {
    return hopSize;
  }

}

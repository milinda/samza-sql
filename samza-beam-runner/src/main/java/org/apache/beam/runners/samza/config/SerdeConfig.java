/**
 * Copyright 2016 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.samza.config;

import org.apache.samza.serializers.Serde;

public class SerdeConfig {
  private final String name;
  private final Class<? extends Serde> serdeClass;

  public SerdeConfig(String name, Class<? extends Serde> serdeClass) {
    this.name = name;
    this.serdeClass = serdeClass;
  }

  public String getName() {
    return name;
  }

  public Class<? extends Serde> getSerdeClass() {
    return serdeClass;
  }
}

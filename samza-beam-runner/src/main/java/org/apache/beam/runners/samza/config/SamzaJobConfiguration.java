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

import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemFactory;

import java.util.Properties;

public class SamzaJobConfiguration {
  private static final String SERIALIZER_PREFIX = "serializers.registry.%s";
  private static final String SERDE = "serializers.registry.%s.class";

  private final MapConfig rawConfig = new MapConfig();

  public SamzaJobConfiguration addSerde(String name, Class<? extends Serde> serdeClass) {
    return this;
  }

  public SamzaJobConfiguration addSystem(String name,
                                         Class<? extends SystemFactory> factory,
                                         Class<? extends Serde> keySerde,
                                         Class<? extends Serde> messageSerde,
                                         Properties properties) {
    return this;
  }

  public SamzaJobConfiguration addStream(String system, String name, Class<? extends Serde> keySerde,
                                         Class<? extends Serde> messageSerde) {
    return this;
  }
}

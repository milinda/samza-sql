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

import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory;
import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.kv.BaseKeyValueStorageEngineFactory;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.task.StreamTask;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.beam.runners.samza.utils.Validations.isNull;
import static org.apache.beam.runners.samza.utils.Validations.isNullOrEmpty;

public class SamzaJobConfiguration extends HashMap<String, String> {
  private static final String SERIALIZER_PREFIX = "serializers.registry.%s";
  private static final String SERIALIZER_SERDE_CLASS = SERIALIZER_PREFIX + ".class";

  private static final String SYSTEM_PREFIX = "systems.%s.";
  private static final String SYSTEM_FACTORY = SYSTEM_PREFIX + "samza.factory";
  private static final String SYSTEM_KEY_SERDE = SYSTEM_PREFIX + "samza.key.serde";
  private static final String SYSTEM_MSG_SERDE = SYSTEM_PREFIX + "samza.msg.serde";
  private static final String SYSTEM_PROPERTY = SYSTEM_PREFIX + "%s";
  private static final String SYSTEM_CONSUMER_OFFSET_DEFAULT = SYSTEM_PREFIX + "samza.offset.default";

  private static final String STREAM_PREFIX = "systems.%s.streams.%s.";
  private static final String STREAM_MSG_SERDE = STREAM_PREFIX + "samza.msg.serde";
  private static final String STREAM_KEY_SERDE = STREAM_PREFIX + "samza.key.serde";
  private static final String STREAM_BOOTSTRAP = STREAM_PREFIX + "samza.bootstrap";
  private static final String STREAM_CONSUMER_RESET_OFFSET = STREAM_PREFIX + "samza.reset.offset";
  private static final String STREAM_CONSUMER_OFFSET_DEFAULT = STREAM_PREFIX + "samza.offset.default";

  private static final String TASK_CLASS = "task.class";
  private static final String INPUT_STREAMS = "task.inputs";
  private static final String INPUT_STREAMS_VALUE_TEMPLATE = "%s,%s.%s";
  private static final String CHECKPOINT_MANAGER_FACTORY = "task.checkpoint.factory";
  private static final String CHECKPOINT_SYSTEM = "task.checkpoint.system";

  private final static String LOCAL_STORAGE_PREFIX = "stores.%s.";
  private final static String LOCAL_STORAGE_FACTORY = LOCAL_STORAGE_PREFIX + "factory";
  private final static String LOCAL_STORAGE_KEY_SERDE = LOCAL_STORAGE_PREFIX + "key.serde";
  private final static String LOCAL_STORAGE_MSG_SERDE = LOCAL_STORAGE_PREFIX + "msg.serde";
  private final static String LOCAL_STORAGE_CHANGELOG_STREAM = LOCAL_STORAGE_PREFIX + "changelog";

  private static final String YARN_PACKAGE_PATH = "yarn.package.path";
  private static final String YARN_CONTAINER_MAX_MEMORY_MB = "yarn.container.memory.mb";
  private static final String YARN_CONTAINER_MAX_CPU_CORES = "yarn.container.cpu.cores";
  private static final String YARN_CONTAINER_COUNT = "yarn.container.count";


  public SamzaJobConfiguration addSerde(String name, Class<? extends Serde> serdeClass) {
    isNullOrEmpty(name, "Serde name");
    isNull(serdeClass, "Serde class");

    put(format(SERIALIZER_SERDE_CLASS, name), serdeClass.getName());

    return this;
  }

  public SamzaJobConfiguration addSystem(String name,
                                         Class<? extends SystemFactory> factory,
                                         String keySerde,
                                         String messageSerde,
                                         MapConfig additionalConfigurations) {
    isNullOrEmpty(name, "System name");
    isNull(factory, "System factory class");

    put(format(SYSTEM_FACTORY, name), factory.getName());

    if (!isNullOrEmpty(keySerde)) {
      isSerdeExists(keySerde);
      put(format(SYSTEM_KEY_SERDE, name), keySerde);
    }

    if (!isNullOrEmpty(messageSerde)) {
      isSerdeExists(messageSerde);
      put(format(SYSTEM_MSG_SERDE, name), messageSerde);
    }

    if (additionalConfigurations != null) {
      for (Entry<String, String> c : additionalConfigurations.entrySet()) {
        put(format(SYSTEM_PROPERTY, name, c.getKey()), c.getValue());
      }
    }

    return this;
  }

  public SamzaJobConfiguration addStream(String system, String name, String keySerde,
                                         String messageSerde, Boolean isBootstrap) {
    isNullOrEmpty(system, "System name");
    isNullOrEmpty(name, "Stream name");

    if (!isNullOrEmpty(keySerde)) {
      isSerdeExists(keySerde);
      put(format(STREAM_KEY_SERDE, system, name), keySerde);
    }

    if (!isNullOrEmpty(messageSerde)) {
      isSerdeExists(messageSerde);
      put(format(STREAM_MSG_SERDE, system, name), messageSerde);
    }

    if (isBootstrap != null) {
      put(format(STREAM_BOOTSTRAP, system, name), isBootstrap.toString());
    }

    return this;
  }

  public SamzaJobConfiguration task(Class<? extends StreamTask> taskClass) {
    isNull(taskClass, "Task class");

    put(TASK_CLASS, taskClass.getName());

    return this;
  }

  public SamzaJobConfiguration addInput(String system, String stream) {
    isNullOrEmpty(system, "System name");
    isNullOrEmpty(stream, "Stream name");

    if (!containsKey(format(SYSTEM_FACTORY, system))) {
      throw new IllegalArgumentException("Cannot find system " + system + ".");
    }

    if (containsKey(INPUT_STREAMS)) {
      put(INPUT_STREAMS, format(INPUT_STREAMS_VALUE_TEMPLATE, get(INPUT_STREAMS), system, stream));
    } else {
      put(INPUT_STREAMS, format("%s.%s", system, stream));
    }

    return this;
  }

  public SamzaJobConfiguration addBroadcastInput(String broadcastInput) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  public SamzaJobConfiguration checkpointingConfig(Class<? extends CheckpointManagerFactory> checkpointFactoryClass, String system) {
    if (checkpointFactoryClass != null) {
      put(CHECKPOINT_MANAGER_FACTORY, checkpointFactoryClass.getName());
    }

    if (checkpointFactoryClass.getName().equals(KafkaCheckpointManagerFactory.class.getName())) {
      put(CHECKPOINT_SYSTEM, system);
    }

    return this;
  }

  public SamzaJobConfiguration addLocalStorage(String name,
                                               Class<? extends BaseKeyValueStorageEngineFactory> storageEngineFactory,
                                               String keySerde, String messageSerde,
                                               String changelogSystem, String changelogStream) {
    isNullOrEmpty(name, "Local storage name");
    isNull(storageEngineFactory, "Storage engine factory");
    isNullOrEmpty(keySerde, "Key Serde");
    isNullOrEmpty(messageSerde, "Message Serde");
    isNullOrEmpty(changelogSystem, "Changelog system");
    isNullOrEmpty(changelogStream, "Changelog stream");

    isSerdeExists(keySerde);
    isSerdeExists(messageSerde);

    if (!containsKey(format(SYSTEM_FACTORY, changelogSystem))) {
      throw new IllegalArgumentException("Cannot find system " + changelogSystem);
    }

    put(format(LOCAL_STORAGE_FACTORY, name), storageEngineFactory.getName());
    put(format(LOCAL_STORAGE_KEY_SERDE, name), keySerde);
    put(format(LOCAL_STORAGE_MSG_SERDE, name), messageSerde);
    put(format(LOCAL_STORAGE_CHANGELOG_STREAM, name), format("%s.%s", changelogSystem, changelogStream));

    return this;
  }

  public SamzaJobConfiguration yarnPackagePath(String packagePath) {
    isNullOrEmpty(packagePath, "YARN package path");

    put(YARN_PACKAGE_PATH, packagePath);

    return this;
  }

  public SamzaJobConfiguration yarnContainerCount(int containerCount) {
    if (containerCount <= 0) {
      throw new IllegalArgumentException("Container count is less than or equal to 0.");
    }

    put(YARN_CONTAINER_COUNT, Integer.toString(containerCount));

    return this;
  }

  private void isSerdeExists(String serde) {
    if(!containsKey(format(SERIALIZER_SERDE_CLASS, serde))) {
      throw new IllegalArgumentException("Cannot find Serde " + serde + ".");
    }
  }
}

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

public class LocalStoreConfiguration {
  private final String name;
  private final String factoryClass;
  private final String keySerde;
  private final String msgSerde;
  private final String changelog;

  public LocalStoreConfiguration(String name, String factoryClass, String keySerde, String msgSerde, String changelog) {
    this.name = name;
    this.factoryClass = factoryClass;
    this.keySerde = keySerde;
    this.msgSerde = msgSerde;
    this.changelog = changelog;
  }

  public String getName() {
    return name;
  }

  public String getFactoryClass() {
    return factoryClass;
  }

  public String getKeySerde() {
    return keySerde;
  }

  public String getMsgSerde() {
    return msgSerde;
  }

  public String getChangelog() {
    return changelog;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.sql.master.model;

import com.avaje.ebean.Model;
import com.avaje.ebean.annotation.CreatedTimestamp;
import com.avaje.ebean.annotation.UpdatedTimestamp;

import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.Version;
import java.sql.Timestamp;

/**
 * Base domain object with Id, version, whenCreated and whenUpdated.
 * <p/>
 * <p/>
 * Extending Model to enable the 'active record' style.
 * <p/>
 * <p/>
 * whenCreated and whenUpdated are generally useful for maintaining external search services (like
 * elasticsearch) and audit.
 */
@MappedSuperclass
public abstract class BaseModel extends Model {

  @Id
  Long id;

  @Version
  Long version;

  @CreatedTimestamp
  Timestamp whenCreated;

  @UpdatedTimestamp
  Timestamp whenUpdated;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getVersion() {
    return version;
  }

  public void setVersion(Long version) {
    this.version = version;
  }

  public Timestamp getWhenCreated() {
    return whenCreated;
  }

  public void setWhenCreated(Timestamp whenCreated) {
    this.whenCreated = whenCreated;
  }

  public Timestamp getWhenUpdated() {
    return whenUpdated;
  }

  public void setWhenUpdated(Timestamp whenUpdated) {
    this.whenUpdated = whenUpdated;
  }

}

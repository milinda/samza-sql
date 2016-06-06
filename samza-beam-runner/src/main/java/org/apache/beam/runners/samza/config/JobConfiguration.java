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

import org.apache.samza.job.StreamJobFactory;

import java.util.Properties;

public class JobConfiguration {
  private final Class<? extends StreamJobFactory> jobFactory;
  private final String name;
  private final String jobId;
  private final SystemConfiguration coordinatorSystem;
  private TaskConfiguration taskConfiguration;

  public JobConfiguration(Class<? extends StreamJobFactory> jobFactory, String name, String jobId, SystemConfiguration coordinatorSystem) {
    this.jobFactory = jobFactory;
    this.name = name;
    this.jobId = jobId;
    this.coordinatorSystem = coordinatorSystem;
  }

  public Class<? extends StreamJobFactory> getJobFactory() {
    return jobFactory;
  }

  public String getName() {
    return name;
  }

  public String getJobId() {
    return jobId;
  }

  public SystemConfiguration getCoordinatorSystem() {
    return coordinatorSystem;
  }

  /**
   * Build properties object from job configuration.
   * @return Samza job properties
   */
  public Properties build(){
    Properties props = new Properties();

    return props;
  }
}

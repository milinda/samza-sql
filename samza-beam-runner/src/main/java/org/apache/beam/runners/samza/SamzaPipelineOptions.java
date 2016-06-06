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

package org.apache.beam.runners.samza;

import org.apache.beam.sdk.options.*;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public interface SamzaPipelineOptions  extends PipelineOptions, StreamingOptions,
    ApplicationNameOptions {

  /**
   * Execution mode define where Samza job get executed and how. THREAD mode uses Samza's
   * {@link org.apache.samza.job.local.ThreadJobFactory} while PROCESS mode uses
   * {@link org.apache.samza.job.local.ProcessJobFactory}. Samza job get executed locally inn both THREAD and PROCESS
   * modes. In YARN mode, Samza job get executed in a remote YARN cluster and ~/.samza/conf will be use as YARN_HOME.
   */
  @Description("Samza job execution mode (YARN, THREAD or PROCESS).")
  @Default.String("THREAD")
  String getExecutionMode();
  void setExecutionMode(String value);

  @Description("Samza job package which is a .tar.gz file with a specific directory structure. Required for YARN jobs.")
  String getJobPackage();
  void setJobPackage(String value);

  @Description("Job name.")
  @Default.InstanceFactory(JobNameFactory.class)
  String getJobName();
  void setJobName(String value);


  class JobNameFactory implements DefaultValueFactory<String> {
    private static final DateTimeFormatter FORMATTER =
        DateTimeFormat.forPattern("MMddHHmmss").withZone(DateTimeZone.UTC);

    @Override
    public String create(PipelineOptions options) {
      String appName = options.as(ApplicationNameOptions.class).getAppName();
      String normalizedAppName = appName == null || appName.length() == 0 ? "SamzaRunner"
          : appName.toLowerCase()
          .replaceAll("[^a-z0-9]", "0")
          .replaceAll("^[^a-z]", "a");
      String userName = System.getProperty("user.name", "");
      String normalizedUserName = userName.toLowerCase()
          .replaceAll("[^a-z0-9]", "0");
      String datePart = FORMATTER.print(DateTimeUtils.currentTimeMillis());
      return normalizedAppName + "-" + normalizedUserName + "-" + datePart;
    }
  }
}

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

import com.google.common.base.Joiner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;

import java.util.ArrayList;

public class SamzaPipelineRunner extends PipelineRunner<SamzaRunnerResult> {

  /**
   * Options used in the context of this pipeline runner.
   */
  private final SamzaPipelineOptions options;

  /**
   * Creates and returns a new SamzaPipelineRunner with default options for running Samza job locally.
   *
   * @return  A pipeline runner with default options.
   */
  public static SamzaPipelineRunner create() {
    SamzaPipelineOptions options = PipelineOptionsFactory.as(SamzaPipelineOptions.class);
    options.setRunner(SamzaPipelineRunner.class);

    return new SamzaPipelineRunner(options);
  }

  /**
   * Creates and returns a new SamzaPipelineRunner with provided options.
   *
   * @param options The SamzaPipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SamzaPipelineRunner create(SamzaPipelineOptions options) {
    return new SamzaPipelineRunner(options);
  }

  /**
   * Creates and returns a new SamzaPipelineRunner with provided options.
   *
   * @param options The PipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SamzaPipelineRunner fromOptions(PipelineOptions options) {
    SamzaPipelineOptions samzaOptions = PipelineOptionsValidator.validate(SamzaPipelineOptions.class, options);

    ArrayList<String> missing = new ArrayList<>();

    if (samzaOptions.getAppName() == null) {
      missing.add("appName");
    }
    if (missing.size() > 0) {
      throw new IllegalArgumentException(
          "Missing required values: " + Joiner.on(',').join(missing));
    }

    return new SamzaPipelineRunner(samzaOptions);
  }

  private SamzaPipelineRunner(SamzaPipelineOptions options) {
    this.options = options;
  }

  @Override
  public SamzaRunnerResult run(Pipeline pipeline) {
    return null;
  }
}

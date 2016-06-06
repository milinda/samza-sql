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

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;

/**
 * Contains the {@link PipelineRunnerRegistrar} and {@link PipelineOptionsRegistrar} for the
 * {@link SamzaRunnerRegistrar}.
 *
 * {@link AutoService} will register Spark's implementations of the {@link PipelineRunner}
 * and {@link PipelineOptions} as available pipeline runner services.
 */
public class SamzaRunnerRegistrar {
  private SamzaRunnerRegistrar() {}

  /**
   * Registers the {@link SamzaPipelineRunner}
   */
  @AutoService(PipelineRunnerRegistrar.class)
  public static class Runner implements PipelineRunnerRegistrar {
    @Override
    public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
      return ImmutableList.<Class<? extends PipelineRunner<?>>>of(SamzaPipelineRunner.class);
    }
  }

  /**
   * Registers the {@link SamzaPipelineOptions}.
   */
  @AutoService(PipelineOptionsRegistrar.class)
  public static class Options implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return ImmutableList.<Class<? extends PipelineOptions>>of(SamzaPipelineOptions.class);
    }
  }
}

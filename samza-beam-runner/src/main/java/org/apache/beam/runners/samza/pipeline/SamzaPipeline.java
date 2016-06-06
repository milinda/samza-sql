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

package org.apache.beam.runners.samza.pipeline;

/**
 * Defines dataflow pipeline that get executed as dag of {@link SamzaJob}s.
 * {@link org.apache.beam.runners.samza.SamzaPipelineRunner}  translate a {@link org.apache.beam.sdk.Pipeline} instance
 * to a {@link SamzaPipeline} instance that can be executed as a dag of {@link SamzaJob}s locally or in a remote
 * YARN cluster.
 */
public class SamzaPipeline {

  public void execute() {

  }
}

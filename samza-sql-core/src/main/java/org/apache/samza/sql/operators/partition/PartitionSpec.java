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

package org.apache.samza.sql.operators.partition;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.expressions.ScalarExpression;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.operators.SimpleOperatorSpec;
import org.apache.samza.system.SystemStream;

import java.util.StringTokenizer;


/**
 * This class defines the specification class of {@link org.apache.samza.sql.operators.partition.PartitionOp}
 *
 */
public class PartitionSpec extends SimpleOperatorSpec implements OperatorSpec {

  /**
   * Partition key generator
   */
  private final ScalarExpression partitionKeyGenerator;

  /**
   * The number of partitions
   */
  private final int parNum;

  /**
   * The <code>SystemStream</code> to send the partition output to
   */
  private final SystemStream sysStream;

  /**
   * Ctor to create the {@code PartitionSpec}
   *
   * @param id The ID of the {@link org.apache.samza.sql.operators.partition.PartitionOp}
   * @param input The input stream name
   * @param output The output {@link org.apache.samza.system.SystemStream} object
   * @param parNum The number of partitions
   * @param partitionByFields Fields to use for generating partition key
   */
  public PartitionSpec(String id, String input, SystemStream output, int parNum,
                       String... partitionByFields) {
    super(id, EntityName.getStreamName(input), EntityName.getStreamName(output.getSystem() + ":" + output.getStream()));
    this.parNum = parNum;
    this.sysStream = output;
    this.partitionKeyGenerator = new FieldBasedPartitionKeyGenerator(partitionByFields);
  }

  /**
   * Ctor to create the {@code PartitionSpec}
   *
   * @param id The ID of the {@link org.apache.samza.sql.operators.partition.PartitionOp}
   * @param input The input stream name
   * @param output The output {@link org.apache.samza.system.SystemStream} object
   * @param parNum The number of partitions
   * @param partitionKeyGenerator The partition key generator
   */
  public PartitionSpec(String id, String input, SystemStream output, int parNum,
                       ScalarExpression partitionKeyGenerator) {
    super(id, EntityName.getStreamName(input), EntityName.getStreamName(output.getSystem() + ":" + output.getStream()));
    this.parNum = parNum;
    this.sysStream = output;
    this.partitionKeyGenerator = partitionKeyGenerator;
  }

  /**
   * Ctor to create the {@code PartitionSpec}
   *
   * @param id The ID of the {@link org.apache.samza.sql.operators.partition.PartitionOp}
   * @param input The input stream name
   * @param output The output {@link org.apache.samza.system.SystemStream} object
   * @param parNum The number of partitions
   * @param partitionKeyGenerator The partition key generator
   */
  public PartitionSpec(String id, EntityName input, EntityName output, int parNum,
                       ScalarExpression partitionKeyGenerator) {
    super(id, input, output);
    this.parNum = parNum;

    String outputStreamName = output.getName();
    StringTokenizer tokenizer = new StringTokenizer(outputStreamName, ":");

    if (tokenizer.countTokens() != 2){
      throw new IllegalArgumentException(String.format("Invalid output entity name: %s",
          outputStreamName));
    }

    this.sysStream = new SystemStream(tokenizer.nextToken(), tokenizer.nextToken());
    this.partitionKeyGenerator = partitionKeyGenerator;
  }

  /**
   * Method to get the partition key generator.
   * @return The partition key generator
   */
  public ScalarExpression getPartitionKeyGenerator(){
    return partitionKeyGenerator;
  }

  /**
   * Method to get the number of partitions
   *
   * @return The number of partitions
   */
  public int getParNum() {
    return this.parNum;
  }

  /**
   * Method to get the output {@link org.apache.samza.system.SystemStream}
   *
   * @return The output system stream object
   */
  public SystemStream getSystemStream() {
    return this.sysStream;
  }
}

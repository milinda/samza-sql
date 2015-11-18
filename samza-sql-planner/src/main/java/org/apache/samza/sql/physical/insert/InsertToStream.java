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

package org.apache.samza.sql.physical.insert;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.data.IntermediateMessageTuple;
import org.apache.samza.sql.data.TupleConverter;
import org.apache.samza.sql.operators.SimpleOperatorImpl;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SimpleMessageCollector;

import java.util.StringTokenizer;

public class InsertToStream extends SimpleOperatorImpl {

    private final InsertToStreamSpec spec;

    private final SystemStream outputStream;

    private final RelDataType type;

    public InsertToStream(InsertToStreamSpec spec, RelDataType type) {
        super(spec);
        this.spec = spec;
        this.type = type;

        if (!spec.getOutputName().isStream()) {
            throw new IllegalArgumentException("Output entity " + spec.getOutputName() + " should be a stream.");
        }

        StringTokenizer tokenizer = new StringTokenizer(spec.getOutputName().getName(), ":");

        if (tokenizer.countTokens() != 2) {
            throw new IllegalArgumentException("Output stream name should be in the form <system>:<stream>, " +
                    "but the given name is " + spec.getOutputName().getName());
        }

        this.outputStream = new SystemStream(tokenizer.nextToken(), tokenizer.nextToken());
    }

    @Override
    protected void realRefresh(long timeNano, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

    }

    @Override
    protected void realProcess(Relation rel, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {

    }

    @Override
    protected void realProcess(Tuple ituple, SimpleMessageCollector collector, TaskCoordinator coordinator) throws Exception {
        // TODO: Current implementation uses object array to avro conversion. But this should be configurable based on stream's type.
        collector.send(new OutgoingMessageEnvelope(outputStream, ituple.getKey(),
                TupleConverter.objectArrayToSamzaData(((IntermediateMessageTuple)ituple).getContent(), type)));
    }

    @Override
    public void init(Config config, TaskContext context) throws Exception {

    }
}

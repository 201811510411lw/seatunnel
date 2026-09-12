/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.translation.flink.sink.FlinkRecoverySink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** State compatibility requires retaining the identities of every existing sink operator. */
class FlinkRecoverySinkTopologyTest {

    @Test
    void shouldPreserveOriginalOperatorIdentitiesAndParallelism() {
        for (int parallelism : new int[] {1, 4}) {
            for (boolean explicitUid : new boolean[] {false, true}) {
                JobGraph original = graph(false, parallelism, explicitUid);
                JobGraph recovery = graph(true, parallelism, explicitUid);
                assertEquals(
                        identities(original),
                        identities(recovery),
                        "Operator identity changed for p=" + parallelism + ", uid=" + explicitUid);
                int operators = 0;
                for (JobVertex vertex : recovery.getVertices()) {
                    operators += vertex.getOperatorIDs().size();
                }
                assertEquals(
                        4, operators, "Source, writer, local and global committers must remain");
            }
        }
    }

    private static JobGraph graph(boolean recovery, int parallelism, boolean explicitUid) {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(parallelism);
        TopologySink original = new TopologySink();
        DataStreamSink<Integer> sink =
                environment
                        .fromElements(1, 2)
                        .uid("topology-source")
                        .sinkTo(
                                recovery
                                        ? new FlinkRecoverySink<>(original, parallelism)
                                        : SinkV1Adapter.wrap(original))
                        .name("topology-sink")
                        .setParallelism(parallelism);
        if (explicitUid) {
            sink.uid("topology-sink");
        }
        return environment.getStreamGraph().getJobGraph();
    }

    private static Map<String, List<String>> identities(JobGraph graph) {
        Map<String, List<String>> identities = new TreeMap<>();
        for (JobVertex vertex : graph.getVertices()) {
            List<String> values = new ArrayList<>();
            values.add(vertex.getID().toString());
            values.add(vertex.getParallelism() + "/" + vertex.getMaxParallelism());
            for (OperatorIDPair identity : vertex.getOperatorIDs()) {
                values.add(
                        identity.getGeneratedOperatorID()
                                + ":"
                                + identity.getUserDefinedOperatorID()
                                        .map(Object::toString)
                                        .orElse(""));
            }
            identities.put(vertex.getName(), values);
        }
        return identities;
    }

    private static final class TopologySink implements Sink<Integer, Integer, Integer, Integer> {

        @Override
        public SinkWriter<Integer, Integer, Integer> createWriter(
                InitContext context, List<Integer> states) {
            throw new UnsupportedOperationException("This test only builds the JobGraph");
        }

        @Override
        public Optional<Committer<Integer>> createCommitter() {
            return Optional.of(
                    new Committer<Integer>() {
                        @Override
                        public List<Integer> commit(List<Integer> committables) {
                            return Collections.emptyList();
                        }

                        @Override
                        public void close() {}
                    });
        }

        @Override
        public Optional<GlobalCommitter<Integer, Integer>> createGlobalCommitter() {
            return Optional.of(
                    new GlobalCommitter<Integer, Integer>() {
                        @Override
                        public List<Integer> filterRecoveredCommittables(
                                List<Integer> committables) {
                            return committables;
                        }

                        @Override
                        public Integer combine(List<Integer> committables) {
                            return committables.size();
                        }

                        @Override
                        public List<Integer> commit(List<Integer> committables) {
                            return Collections.emptyList();
                        }

                        @Override
                        public void endOfInput() {}

                        @Override
                        public void close() {}
                    });
        }

        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getWriterStateSerializer() {
            return getCommittableSerializer();
        }

        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getGlobalCommittableSerializer() {
            return getCommittableSerializer();
        }

        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getCommittableSerializer() {
            return Optional.of(
                    new SimpleVersionedSerializer<Integer>() {
                        @Override
                        public int getVersion() {
                            return 1;
                        }

                        @Override
                        public byte[] serialize(Integer value) {
                            return ByteBuffer.allocate(4).putInt(value).array();
                        }

                        @Override
                        public Integer deserialize(int version, byte[] bytes) {
                            return ByteBuffer.wrap(bytes).getInt();
                        }
                    });
        }
    }
}

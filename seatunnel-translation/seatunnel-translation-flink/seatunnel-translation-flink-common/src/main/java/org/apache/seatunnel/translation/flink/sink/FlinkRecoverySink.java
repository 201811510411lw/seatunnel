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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/** Retains Flink's writer and local committer adapters, replacing only global commit recovery. */
public final class FlinkRecoverySink<InputT, CommT, WriterStateT, GlobalCommT>
        implements StatefulSink<InputT, WriterStateT>, WithPostCommitTopology<InputT, CommT> {

    private final org.apache.flink.api.connector.sink.Sink<InputT, CommT, WriterStateT, GlobalCommT>
            original;
    private final StatefulSink<InputT, WriterStateT> stateful;
    private final TwoPhaseCommittingSink<InputT, CommT> committing;
    private final int writerParallelism;
    private final int legacyWriterParallelism;
    private final long legacyCheckpointId;

    public FlinkRecoverySink(
            org.apache.flink.api.connector.sink.Sink<InputT, CommT, WriterStateT, GlobalCommT>
                    original,
            int writerParallelism) {
        this(original, writerParallelism, 0, -1);
    }

    @SuppressWarnings("unchecked")
    public FlinkRecoverySink(
            org.apache.flink.api.connector.sink.Sink<InputT, CommT, WriterStateT, GlobalCommT>
                    original,
            int writerParallelism,
            int legacyWriterParallelism,
            long legacyCheckpointId) {
        this.original = original;
        this.writerParallelism = writerParallelism;
        this.legacyWriterParallelism = legacyWriterParallelism;
        this.legacyCheckpointId = legacyCheckpointId;
        Sink<InputT> adapted = SinkV1Adapter.wrap(original);
        stateful = (StatefulSink<InputT, WriterStateT>) adapted;
        committing = (TwoPhaseCommittingSink<InputT, CommT>) adapted;
    }

    @Override
    public Writer<InputT, CommT, WriterStateT> createWriter(Sink.InitContext context)
            throws IOException {
        return new Writer<>(stateful.createWriter(context));
    }

    @Override
    public Writer<InputT, CommT, WriterStateT> restoreWriter(
            Sink.InitContext context, Collection<WriterStateT> states) throws IOException {
        return new Writer<>(stateful.restoreWriter(context, states));
    }

    @Override
    public SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer() {
        return stateful.getWriterStateSerializer();
    }

    @Override
    public Committer<CommT> createCommitter() throws IOException {
        return committing.createCommitter();
    }

    @Override
    public SimpleVersionedSerializer<CommT> getCommittableSerializer() {
        return committing.getCommittableSerializer();
    }

    @Override
    public void addPostCommitTopology(DataStream<CommittableMessage<CommT>> input) {
        FlinkRecoveryGlobalCommitterOperator<CommT, GlobalCommT> operator =
                new FlinkRecoveryGlobalCommitterOperator<>(
                        original, writerParallelism, legacyWriterParallelism, legacyCheckpointId);
        operator.setChainingStrategy(ChainingStrategy.ALWAYS);
        input.global()
                .transform("Global Committer", Types.VOID, operator)
                .setParallelism(1)
                .setMaxParallelism(1);
    }

    public static final class Writer<InputT, CommT, WriterStateT>
            implements StatefulSink.StatefulSinkWriter<InputT, WriterStateT>,
                    TwoPhaseCommittingSink.PrecommittingSinkWriter<InputT, CommT> {

        private final StatefulSink.StatefulSinkWriter<InputT, WriterStateT> stateful;
        private final TwoPhaseCommittingSink.PrecommittingSinkWriter<InputT, CommT> committing;

        @SuppressWarnings("unchecked")
        private Writer(StatefulSink.StatefulSinkWriter<InputT, WriterStateT> original) {
            stateful = original;
            committing = (TwoPhaseCommittingSink.PrecommittingSinkWriter<InputT, CommT>) original;
        }

        @Override
        public void write(InputT value, SinkWriter.Context context)
                throws IOException, InterruptedException {
            stateful.write(value, context);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            stateful.flush(endOfInput);
        }

        @Override
        public List<WriterStateT> snapshotState(long checkpointId) throws IOException {
            return stateful.snapshotState(checkpointId);
        }

        @Override
        public Collection<CommT> prepareCommit() throws IOException, InterruptedException {
            return committing.prepareCommit();
        }

        @Override
        public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
            stateful.writeWatermark(watermark);
        }

        @Override
        public void close() throws Exception {
            stateful.close();
        }
    }
}

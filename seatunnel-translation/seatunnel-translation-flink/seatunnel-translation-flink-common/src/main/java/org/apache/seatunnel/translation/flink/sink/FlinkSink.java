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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportSinkGlobalCommitRecovery;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.serialization.CommitWrapperSerializer;
import org.apache.seatunnel.translation.flink.serialization.FlinkSimpleVersionedSerializer;
import org.apache.seatunnel.translation.flink.serialization.FlinkWriterStateSerializer;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.sql.DriverManager;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The sink implementation of {@link Sink}, the entrypoint of flink sink translation
 *
 * @param <InputT> The generic type of input data
 * @param <CommT> The generic type of commit message
 * @param <WriterStateT> The generic type of writer state
 * @param <GlobalCommT> The generic type of global commit message
 */
public class FlinkSink<InputT, CommT, WriterStateT, GlobalCommT>
        implements Sink<InputT, CommitWrapper<CommT>, FlinkWriterState<WriterStateT>, GlobalCommT> {

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }

    private final SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT> sink;

    private final List<CatalogTable> catalogTables;

    private final int parallelism;

    private final boolean requiresGlobalCommitRecovery;

    public FlinkSink(
            SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT> sink,
            List<CatalogTable> catalogTables,
            int parallelism) {
        this.sink = sink;
        this.catalogTables = catalogTables;
        this.parallelism = parallelism;
        this.requiresGlobalCommitRecovery = SupportSinkGlobalCommitRecovery.isRequired(sink);
    }

    /** Returns the recovery contract selected once after engine routing setup. */
    public boolean requiresGlobalCommitRecovery() {
        return requiresGlobalCommitRecovery;
    }

    @Override
    public SinkWriter<InputT, CommitWrapper<CommT>, FlinkWriterState<WriterStateT>> createWriter(
            Sink.InitContext context, List<FlinkWriterState<WriterStateT>> states)
            throws IOException {
        org.apache.seatunnel.api.sink.SinkWriter.Context stContext =
                new FlinkSinkWriterContext(context, parallelism);
        if (states == null || states.isEmpty()) {
            if (context.getRestoredCheckpointId().isPresent() && requiresGlobalCommitRecovery) {
                throw new IllegalStateException(
                        "Cannot restore global-commit recovery sink without writer state; "
                                + "legacy savepoints and rescaled empty assignments require a fresh snapshot");
            }
            return new FlinkSinkWriter<>(sink.createWriter(stContext), 1, stContext);
        } else {
            if (requiresGlobalCommitRecovery
                    && states.stream()
                            .anyMatch(
                                    state ->
                                            state.getCheckpointId()
                                                    != states.get(0).getCheckpointId())) {
                throw new IllegalStateException(
                        "Cannot merge global-commit recovery writer states from different checkpoints");
            }
            List<WriterStateT> restoredState =
                    states.stream().map(FlinkWriterState::getState).collect(Collectors.toList());
            return new FlinkSinkWriter<>(
                    sink.restoreWriter(stContext, restoredState),
                    states.get(0).getCheckpointId() + 1,
                    stContext);
        }
    }

    @Override
    public Optional<Committer<CommitWrapper<CommT>>> createCommitter() throws IOException {
        Optional<Committer<CommitWrapper<CommT>>> committer =
                sink.createCommitter().map(FlinkCommitter::new);
        if (requiresGlobalCommitRecovery) {
            requireGlobalCommitRecoverySupport();
            if (!committer.isPresent()) {
                // SinkV1Adapter otherwise selects its stateless global-only branch. Retain writer
                // state and forward each complete checkpoint to the real global committer.
                return Optional.of(new RecoveryStateCommitter<>());
            }
        }
        return committer;
    }

    private static final class RecoveryStateCommitter<CommitT> implements Committer<CommitT> {

        @Override
        public List<CommitT> commit(List<CommitT> committables) {
            return java.util.Collections.emptyList();
        }

        @Override
        public void close() {}
    }

    @Override
    public Optional<GlobalCommitter<CommitWrapper<CommT>, GlobalCommT>> createGlobalCommitter()
            throws IOException {
        Optional<GlobalCommitter<CommitWrapper<CommT>, GlobalCommT>> globalCommitter =
                sink.createAggregatedCommitter()
                        .map(
                                committer ->
                                        new FlinkGlobalCommitter<>(
                                                committer, requiresGlobalCommitRecovery));
        if (requiresGlobalCommitRecovery && !globalCommitter.isPresent()) {
            throw new IllegalStateException(
                    "Global commit recovery requires an aggregated committer");
        }
        return globalCommitter;
    }

    @Override
    public Optional<SimpleVersionedSerializer<CommitWrapper<CommT>>> getCommittableSerializer() {
        try {
            if (sink.createCommitter().isPresent()
                    || sink.createAggregatedCommitter().isPresent()) {
                Optional<SimpleVersionedSerializer<CommitWrapper<CommT>>> serializer =
                        sink.getCommitInfoSerializer().map(CommitWrapperSerializer::new);
                if (requiresGlobalCommitRecovery && !serializer.isPresent()) {
                    throw new IllegalStateException(
                            "Global commit recovery requires a commit info serializer");
                }
                return serializer;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create Committer or AggregatedCommitter", e);
        }
        if (requiresGlobalCommitRecovery) {
            throw new IllegalStateException(
                    "Global commit recovery requires a commit info serializer");
        }
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<GlobalCommT>> getGlobalCommittableSerializer() {
        try {
            if (sink.createAggregatedCommitter().isPresent()) {
                Optional<SimpleVersionedSerializer<GlobalCommT>> serializer =
                        sink.getAggregatedCommitInfoSerializer()
                                .map(FlinkSimpleVersionedSerializer::new);
                if (requiresGlobalCommitRecovery && !serializer.isPresent()) {
                    throw new IllegalStateException(
                            "Global commit recovery requires an aggregated commit info serializer");
                }
                return serializer;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create AggregatedCommitter", e);
        }
        if (requiresGlobalCommitRecovery) {
            throw new IllegalStateException(
                    "Global commit recovery requires an aggregated committer");
        }
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<FlinkWriterState<WriterStateT>>>
            getWriterStateSerializer() {
        Optional<SimpleVersionedSerializer<FlinkWriterState<WriterStateT>>> serializer =
                sink.getWriterStateSerializer().map(FlinkWriterStateSerializer::new);
        if (requiresGlobalCommitRecovery && !serializer.isPresent()) {
            throw new IllegalStateException(
                    "Global commit recovery requires a writer state serializer");
        }
        return serializer;
    }

    private void requireGlobalCommitRecoverySupport() throws IOException {
        if (!sink.getWriterStateSerializer().isPresent()) {
            throw new IllegalStateException(
                    "Global commit recovery requires a writer state serializer");
        }
        if (!sink.getCommitInfoSerializer().isPresent()) {
            throw new IllegalStateException(
                    "Global commit recovery requires a commit info serializer");
        }
        if (!sink.getAggregatedCommitInfoSerializer().isPresent()) {
            throw new IllegalStateException(
                    "Global commit recovery requires an aggregated commit info serializer");
        }
        if (!sink.createAggregatedCommitter().isPresent()) {
            throw new IllegalStateException(
                    "Global commit recovery requires an aggregated committer");
        }
    }
}

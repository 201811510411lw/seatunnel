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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonCommitInfo;
import org.apache.seatunnel.translation.flink.sink.FlinkSink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises a completed checkpoint whose nonempty local commits never reached the global sink. */
class PaimonFlinkPendingRecoveryTest {

    private static final int WRITERS = 4;
    private static final int ROWS = 128;
    private static final Map<String, FailureControl> CONTROLS = new ConcurrentHashMap<>();
    private static final Map<String, List<Long>> RESTORE_COMMIT_NANOS = new ConcurrentHashMap<>();

    @TempDir private Path temporaryDirectory;

    static void registerFailureControl(String runId, int writers) {
        CONTROLS.put(runId, new FailureControl(writers));
    }

    static void removeFailureControl(String runId) {
        CONTROLS.remove(runId);
    }

    static boolean wasFailureInjected(String runId) {
        return CONTROLS.get(runId).injected.get();
    }

    static int failureFragments(String runId) {
        return CONTROLS.get(runId).fragments.get();
    }

    static Set<Long> failureCommitBoundaries(String runId) {
        return Collections.unmodifiableSet(CONTROLS.get(runId).commitBoundaries);
    }

    static long restoredCheckpoint(String runId) {
        return CONTROLS.get(runId).restoredCheckpoint;
    }

    static int completedAfterRestore(String runId) {
        return CONTROLS.get(runId).completedAfterRestore.get();
    }

    static RichParallelSourceFunction<SeaTunnelRow> pendingSource(String runId) {
        return new PendingSource(runId);
    }

    static PaimonSink failingSink(ReadonlyConfig config, CatalogTable table, String runId) {
        return new FailingBeforeLocalEmitSink(config, table, runId);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static DataStreamSink<SeaTunnelRow> legacyRoutedSink(
            DataStream<SeaTunnelRow> input,
            SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sink,
            List<CatalogTable> tables,
            int parallelism) {
        return input.partitionCustom(
                        new SinkWriteRoutingPartitioner(),
                        new SinkWriteRoutingPartitioner.RoutingKeySelector(
                                sink.getWriteRouting().get(), parallelism))
                .sinkTo(SinkV1Adapter.wrap(new FlinkSink<>(sink, tables, parallelism)))
                .name(sink.getPluginName() + "-Sink")
                .setParallelism(parallelism);
    }

    @Test
    @Timeout(120)
    void shouldRecoverAllPendingWritersBeforeNextCheckpoint() throws Exception {
        RESTORE_COMMIT_NANOS.clear();
        String runId = UUID.randomUUID().toString();
        RESTORE_COMMIT_NANOS.put(runId, Collections.synchronizedList(new ArrayList<>()));
        FailureControl control = new FailureControl(WRITERS);
        CONTROLS.put(runId, control);
        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", temporaryDirectory.resolve("warehouse").toString());
        properties.put("plugin_name", "Paimon");
        properties.put("database", "pending_recovery");
        properties.put("table", "pending_rows");
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "16");
        options.put("write-only", "true");
        options.put("commit.timeout", "5 s");
        properties.put("paimon.table.write-props", options);
        ReadonlyConfig config = ReadonlyConfig.fromMap(properties);
        TablePath path = TablePath.of("pending_recovery", "pending_rows");
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "value",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                        .build();
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("paimon", "pending_recovery", "pending_rows"),
                        schema,
                        new HashMap<>(),
                        Collections.emptyList(),
                        "pending checkpoint recovery regression");
        PaimonCatalog catalog = new PaimonCatalog("paimon", config);
        catalog.open();
        JobClient client = null;
        try {
            catalog.createDatabase(path, true);
            catalog.createTable(path, table, false);
            JobContext jobContext = new JobContext(113L);
            jobContext.setJobMode(JobMode.STREAMING);
            jobContext.setEnableCheckpoint(true);
            PaimonSink sink = new FailingBeforeLocalEmitSink(config, table, runId);
            sink.setJobContext(jobContext);
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.createLocalEnvironment(WRITERS);
            environment.getConfig().disableClosureCleaner();
            environment.enableCheckpointing(500);
            environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
            DataStream<SeaTunnelRow> input =
                    environment
                            .addSource(new PendingSource(runId))
                            .uid("pending-source")
                            .setParallelism(1);
            SinkExecuteProcessor processor =
                    new SinkExecuteProcessor(
                            new ArrayList<>(),
                            ConfigFactory.empty(),
                            Collections.emptyList(),
                            jobContext);
            processor
                    .createVersionSpecificDataStreamSink(
                            new DataStreamTableInfo(
                                    input, Collections.singletonList(table), "input"),
                            sink,
                            WRITERS,
                            ConfigFactory.empty())
                    .uid("pending-sink");
            client = environment.executeAsync("paimon-pending-recovery");

            Map<Integer, String> expected = new LinkedHashMap<>();
            for (int id = 0; id < ROWS; id++) {
                expected.put(id, "pending-" + id);
            }
            expected.put(0, "after-recovery");
            expected.remove(1);
            expected.put(ROWS, "inserted-after-recovery");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(75);
            Map<Integer, String> actual = Collections.emptyMap();
            while (System.nanoTime() < deadline) {
                if (client.getJobExecutionResult().isDone()) {
                    try {
                        client.getJobExecutionResult().get(5, TimeUnit.SECONDS);
                    } catch (Exception failure) {
                        throw new AssertionError(
                                "Recovery failed: injected="
                                        + control.injected.get()
                                        + ", nonemptyLocalFragments="
                                        + control.fragments.get()
                                        + ", commitBoundaries="
                                        + control.commitBoundaries
                                        + ", restoredFlinkCheckpoint="
                                        + control.restoredCheckpoint,
                                failure);
                    }
                }
                actual = readRows((FileStoreTable) catalog.getPaimonTable(path));
                if (control.completedAfterRestore.get() >= 3 && expected.equals(actual)) {
                    break;
                }
                Thread.sleep(100);
            }
            assertTrue(
                    control.injected.get(),
                    "The failure must occur before local committables are emitted");
            assertEquals(WRITERS, control.fragments.get());
            assertEquals(1, control.commitBoundaries.size());
            assertTrue(
                    control.restoredCheckpoint > 0, "The job must restore a completed checkpoint");
            assertTrue(
                    control.completedAfterRestore.get() >= 3,
                    "Recovery must allow new checkpoints");
            assertEquals(
                    expected,
                    actual,
                    "All pending writers and subsequent I/U/D must survive recovery");
            List<Long> restoreCommitNanos = new ArrayList<>(RESTORE_COMMIT_NANOS.get(runId));
            assertTrue(
                    !restoreCommitNanos.isEmpty(),
                    "Recovery must successfully invoke the actual aggregated restoreCommit");
            System.out.println(
                    "PAIMON_RECOVERY_COMMIT_METRICS {\"successfulCalls\":"
                            + restoreCommitNanos.size()
                            + ",\"durationNanos\":"
                            + restoreCommitNanos
                            + ",\"totalNanos\":"
                            + restoreCommitNanos.stream().mapToLong(Long::longValue).sum()
                            + "}");
        } finally {
            if (client != null && !client.getJobExecutionResult().isDone()) {
                client.cancel().get(20, TimeUnit.SECONDS);
            }
            catalog.close();
            CONTROLS.remove(runId);
            RESTORE_COMMIT_NANOS.remove(runId);
        }
    }

    static Map<Integer, String> readRows(FileStoreTable table) throws Exception {
        ReadBuilder read = table.newReadBuilder();
        Map<Integer, String> rows = new LinkedHashMap<>();
        try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan());
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                rows.put(row.getInt(0), row.getString(1).toString());
            }
        }
        return rows;
    }

    private static final class FailureControl {
        private final CountDownLatch allLocalCommits;
        private final AtomicBoolean injected = new AtomicBoolean();
        private final AtomicInteger fragments = new AtomicInteger();
        private final AtomicInteger completedAfterRestore = new AtomicInteger();
        private final Set<Long> commitBoundaries = ConcurrentHashMap.newKeySet();
        private volatile long restoredCheckpoint = -1;

        private FailureControl(int writers) {
            allLocalCommits = new CountDownLatch(writers);
        }

        private void failBeforeEmit(List<PaimonCommitInfo> infos) throws IOException {
            if (injected.get()
                    || infos.stream()
                            .flatMap(info -> info.getCommittables().stream())
                            .noneMatch(message -> !((CommitMessageImpl) message).isEmpty())) {
                return;
            }
            fragments.incrementAndGet();
            infos.forEach(info -> commitBoundaries.add(info.getCheckpointId()));
            allLocalCommits.countDown();
            try {
                if (!allLocalCommits.await(15, TimeUnit.SECONDS)) {
                    throw new IOException(
                            "Test did not observe a nonempty commit from every writer");
                }
                injected.set(true);
                throw new IOException(
                        "Injected failure after checkpoint completion, before local commit emit");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
        }
    }

    /** Matches the routed no-op local commit contract, with a one-shot failure before emission. */
    private static final class FailingBeforeLocalEmitSink extends PaimonSink {
        private final String runId;

        private FailingBeforeLocalEmitSink(
                ReadonlyConfig config, CatalogTable table, String runId) {
            super(config, table);
            this.runId = runId;
        }

        @Override
        public Optional<SinkCommitter<PaimonCommitInfo>> createCommitter() {
            return Optional.of(new FailingLocalCommitter(runId));
        }

        @Override
        public Optional<SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo>>
                createAggregatedCommitter() throws IOException {
            Optional<SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo>>
                    committer = super.createAggregatedCommitter();
            // Other tests sharing the failure helper keep the original committer.
            return RESTORE_COMMIT_NANOS.containsKey(runId)
                    ? committer.map(delegate -> new TimedAggregatedCommitter(delegate, runId))
                    : committer;
        }
    }

    private static final class TimedAggregatedCommitter
            implements SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo>,
                    SupportMultiTableSinkAggregatedCommitter<Object> {
        private final SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo>
                delegate;
        private final String runId;

        private TimedAggregatedCommitter(
                SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo> delegate,
                String runId) {
            this.delegate = delegate;
            this.runId = runId;
        }

        @Override
        public void init() {
            delegate.init();
        }

        @Override
        public List<PaimonAggregatedCommitInfo> restoreCommit(
                List<PaimonAggregatedCommitInfo> commits) throws IOException {
            long started = System.nanoTime();
            List<PaimonAggregatedCommitInfo> remaining = delegate.restoreCommit(commits);
            long elapsed = System.nanoTime() - started;
            if (remaining == null || remaining.isEmpty()) {
                RESTORE_COMMIT_NANOS.get(runId).add(elapsed);
            }
            return remaining;
        }

        @Override
        public List<PaimonAggregatedCommitInfo> commit(List<PaimonAggregatedCommitInfo> commits)
                throws IOException {
            return delegate.commit(commits);
        }

        @Override
        public PaimonAggregatedCommitInfo combine(List<PaimonCommitInfo> commits) {
            return delegate.combine(commits);
        }

        @Override
        public void abort(List<PaimonAggregatedCommitInfo> commits) throws Exception {
            delegate.abort(commits);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class FailingLocalCommitter implements SinkCommitter<PaimonCommitInfo> {
        private final String runId;

        private FailingLocalCommitter(String runId) {
            this.runId = runId;
        }

        @Override
        public List<PaimonCommitInfo> commit(List<PaimonCommitInfo> commitInfos)
                throws IOException {
            CONTROLS.get(runId).failBeforeEmit(commitInfos);
            return Collections.emptyList();
        }

        @Override
        public void abort(List<PaimonCommitInfo> commitInfos) {}
    }

    private static final class PendingSource extends RichParallelSourceFunction<SeaTunnelRow>
            implements CheckpointedFunction, CheckpointListener {
        private final String runId;
        private volatile boolean running = true;
        private boolean restored;
        private transient ListState<Boolean> state;

        private PendingSource(String runId) {
            this.runId = runId;
        }

        @Override
        public void run(SourceContext<SeaTunnelRow> context) throws Exception {
            synchronized (context.getCheckpointLock()) {
                if (restored) {
                    emit(context, RowKind.UPDATE_AFTER, 0, "after-recovery");
                    emit(context, RowKind.DELETE, 1, "pending-1");
                    emit(context, RowKind.INSERT, ROWS, "inserted-after-recovery");
                } else {
                    for (int id = 0; id < ROWS; id++) {
                        emit(context, RowKind.INSERT, id, "pending-" + id);
                    }
                }
            }
            while (running) {
                Thread.sleep(10);
            }
        }

        private static void emit(
                SourceContext<SeaTunnelRow> context, RowKind kind, int id, String value) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, value});
            row.setRowKind(kind);
            context.collect(row);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.update(Collections.singletonList(true));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("emitted", Boolean.class));
            restored = context.isRestored();
            if (restored) {
                assertTrue(state.get().iterator().hasNext());
                CONTROLS.get(runId).restoredCheckpoint =
                        context.getRestoredCheckpointId().getAsLong();
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (restored) {
                CONTROLS.get(runId).completedAfterRestore.incrementAndGet();
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

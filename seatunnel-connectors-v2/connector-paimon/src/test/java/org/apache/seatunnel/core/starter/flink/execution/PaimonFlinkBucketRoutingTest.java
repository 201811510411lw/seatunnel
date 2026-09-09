package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriteRouting;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonFlinkBucketRoutingTest {

    @TempDir private Path temporaryDirectory;

    @Test
    @Timeout(120)
    void shouldRouteSingleBucketThroughActualFlinkSink() throws Exception {
        runPipeline(1, false);
    }

    @Test
    @Timeout(120)
    void shouldRouteMultipleTablesPartitionsAndBucketsThroughActualFlinkSink() throws Exception {
        runPipeline(4, true);
    }

    @Test
    @Timeout(180)
    void shouldRestoreStreamingSavepointWithTwoWriters() throws Exception {
        runPipeline(4, true, true);
    }

    @Test
    @Timeout(180)
    void shouldRescaleSingleBucketAfterIndependentCompaction() throws Exception {
        runPipeline(1, false, true, 1);
    }

    @Test
    @Timeout(180)
    void shouldRescaleMultipleTablesAfterIndependentCompaction() throws Exception {
        runPipeline(4, true, true, 1);
    }

    private void runPipeline(int buckets, boolean multiTable) throws Exception {
        runPipeline(buckets, multiTable, false);
    }

    private void runPipeline(int buckets, boolean multiTable, boolean streaming) throws Exception {
        runPipeline(buckets, multiTable, streaming, 2);
    }

    private void runPipeline(
            int buckets, boolean multiTable, boolean streaming, int restoredParallelism)
            throws Exception {
        JobContext jobContext = new JobContext(113L);
        jobContext.setJobMode(JobMode.STREAMING);
        jobContext.setEnableCheckpoint(true);
        Map<TablePath, SeaTunnelSink> sinks = new LinkedHashMap<>();
        List<CatalogTable> tables = new ArrayList<>();
        List<PaimonCatalog> catalogs = new ArrayList<>();
        List<ReadonlyConfig> restoredSinkConfigs = new ArrayList<>();
        List<SeaTunnelRow> rows = new ArrayList<>();
        try {
            for (int tableIndex = 0; tableIndex < (multiTable ? 2 : 1); tableIndex++) {
                String tableName = "table_" + tableIndex;
                Map<String, Object> properties = new HashMap<>();
                properties.put("warehouse", temporaryDirectory.resolve("warehouse").toString());
                properties.put("plugin_name", "Paimon");
                properties.put("database", "routing_test");
                properties.put("table", tableName);
                Map<String, String> options = new HashMap<>();
                options.put("bucket", String.valueOf(buckets));
                options.put("write-only", "true");
                properties.put("paimon.table.write-props", options);
                ReadonlyConfig config = ReadonlyConfig.fromMap(properties);
                Map<String, Object> restoredProperties = new HashMap<>(properties);
                restoredProperties.remove("paimon.table.write-props");
                restoredSinkConfigs.add(ReadonlyConfig.fromMap(restoredProperties));
                PaimonCatalog catalog = new PaimonCatalog("paimon", config);
                catalog.open();
                catalogs.add(catalog);
                TablePath tablePath = TablePath.of("routing_test", tableName);
                catalog.createDatabase(tablePath, true);
                TableSchema schema =
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id",
                                                BasicType.INT_TYPE,
                                                (Long) null,
                                                false,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "part",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                false,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "value",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null))
                                .primaryKey(PrimaryKey.of("pk", Arrays.asList("id", "part")))
                                .build();
                CatalogTable table =
                        CatalogTable.of(
                                TableIdentifier.of("paimon", "routing_test", tableName),
                                schema,
                                new HashMap<>(),
                                multiTable
                                        ? Collections.singletonList("part")
                                        : Collections.emptyList(),
                                "routing regression");
                catalog.createTable(tablePath, table, false);
                tables.add(table);
                PaimonSink sink = new TestPaimonSink(config, table);
                sink.setJobContext(jobContext);
                sinks.put(tablePath, sink);
                for (int identifier = 0; identifier < 100; identifier++) {
                    rows.add(row(tablePath, RowKind.INSERT, identifier, "before"));
                }
                rows.add(row(tablePath, RowKind.DELETE, 99, "before"));
                for (int update = 0; update < 4; update++) {
                    rows.add(row(tablePath, RowKind.UPDATE_AFTER, 98, "after-" + update));
                }
            }
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.createLocalEnvironment(2);
            environment.getConfig().disableClosureCleaner();
            environment.setRestartStrategy(
                    org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart());
            DataStream<SeaTunnelRow> input;
            if (streaming) {
                environment.enableCheckpointing(200);
                input =
                        environment
                                .addSource(new SnapshotThenIncrementalSource(rows, false))
                                .uid("routing-source")
                                .setParallelism(2);
            } else {
                input =
                        environment
                                .fromCollection(rows)
                                .partitionCustom(
                                        (Partitioner<Integer>) (writer, count) -> writer,
                                        (KeySelector<SeaTunnelRow, Integer>)
                                                value ->
                                                        value.getRowKind() == RowKind.INSERT
                                                                ? 0
                                                                : 1)
                                .map(
                                        (MapFunction<SeaTunnelRow, SeaTunnelRow>)
                                                value -> {
                                                    if (value.getRowKind() != RowKind.INSERT) {
                                                        Thread.sleep(200);
                                                    }
                                                    return value;
                                                })
                                .setParallelism(2);
            }
            SeaTunnelSink sink =
                    multiTable
                            ? new MultiTableSink(
                                    new MultiTableFactoryContext(
                                            ReadonlyConfig.fromMap(Collections.emptyMap()),
                                            getClass().getClassLoader(),
                                            sinks))
                            : sinks.values().iterator().next();
            SinkExecuteProcessor processor =
                    new SinkExecuteProcessor(
                            new ArrayList<>(),
                            ConfigFactory.empty(),
                            Collections.emptyList(),
                            jobContext);
            processor
                    .createVersionSpecificDataStreamSink(
                            new DataStreamTableInfo(input, tables, "input"),
                            sink,
                            2,
                            ConfigFactory.empty())
                    .uid("routing-sink");
            if (streaming) {
                JobClient client = environment.executeAsync("issue113-streaming-routing");
                String savepoint;
                try {
                    awaitRows(catalogs, sinks, "after-3", client);
                    savepoint =
                            client.stopWithSavepoint(
                                            false,
                                            temporaryDirectory
                                                    .resolve("savepoints")
                                                    .toUri()
                                                    .toString(),
                                            SavepointFormatType.CANONICAL)
                                    .get(45, TimeUnit.SECONDS);
                    client.getJobExecutionResult().get(45, TimeUnit.SECONDS);
                } finally {
                    if (!client.getJobExecutionResult().isDone()) {
                        client.cancel().get(30, TimeUnit.SECONDS);
                    }
                }
                Configuration restoredConfig = new Configuration();
                if (restoredParallelism == 1) {
                    int tableIndex = 0;
                    for (TablePath tablePath : sinks.keySet()) {
                        FileStoreTable table =
                                (FileStoreTable) catalogs.get(tableIndex).getPaimonTable(tablePath);
                        compactWhilePaused(table);
                        new SchemaManager(table.fileIO(), table.location())
                                .commitChanges(SchemaChange.setOption("write-only", "false"));
                        catalogs.get(tableIndex).close();
                        catalogs.get(tableIndex).open();
                        FileStoreTable updated =
                                (FileStoreTable) catalogs.get(tableIndex).getPaimonTable(tablePath);
                        assertFalse(updated.coreOptions().writeOnly());
                        PaimonSink newSink =
                                new PaimonSink(
                                        restoredSinkConfigs.get(tableIndex),
                                        tables.get(tableIndex));
                        newSink.setJobContext(jobContext);
                        sinks.put(tablePath, newSink);
                        tableIndex++;
                    }
                    sink =
                            multiTable
                                    ? new MultiTableSink(
                                            new MultiTableFactoryContext(
                                                    ReadonlyConfig.fromMap(Collections.emptyMap()),
                                                    getClass().getClassLoader(),
                                                    sinks))
                                    : sinks.values().iterator().next();
                }
                restoredConfig.setString("execution.savepoint.path", savepoint);
                StreamExecutionEnvironment restored =
                        StreamExecutionEnvironment.createLocalEnvironment(2, restoredConfig);
                restored.getConfig().disableClosureCleaner();
                restored.setRestartStrategy(
                        org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart());
                restored.enableCheckpointing(200);
                DataStream<SeaTunnelRow> resumed =
                        restored.addSource(new SnapshotThenIncrementalSource(rows, false, true))
                                .uid("routing-source")
                                .setParallelism(restoredParallelism);
                processor
                        .createVersionSpecificDataStreamSink(
                                new DataStreamTableInfo(resumed, tables, "input"),
                                sink,
                                restoredParallelism,
                                ConfigFactory.empty())
                        .uid("routing-sink");
                JobClient resumedClient = restored.executeAsync("issue113-restored-routing");
                try {
                    awaitRows(catalogs, sinks, "restored", resumedClient);
                } finally {
                    if (!resumedClient.getJobExecutionResult().isDone()) {
                        resumedClient.cancel().get(30, TimeUnit.SECONDS);
                    }
                }
            } else {
                environment.execute("issue113-bucket-routing");
            }
            int tableIndex = 0;
            for (TablePath tablePath : sinks.keySet()) {
                FileStoreTable table =
                        (FileStoreTable) catalogs.get(tableIndex++).getPaimonTable(tablePath);
                ReadBuilder read = table.newReadBuilder();
                Map<Integer, String> actual = new HashMap<>();
                try (RecordReader<InternalRow> reader =
                                read.newRead().createReader(read.newScan().plan());
                        RecordReaderIterator<InternalRow> iterator =
                                new RecordReaderIterator<>(reader)) {
                    while (iterator.hasNext()) {
                        InternalRow value = iterator.next();
                        actual.put(value.getInt(0), value.getString(2).toString());
                    }
                }
                assertEquals(99, actual.size());
                assertFalse(actual.containsKey(99));
                assertEquals(streaming ? "restored" : "after-3", actual.get(98));
                if (streaming) {
                    assertFalse(actual.containsKey(97));
                    assertEquals("new-after-restore", actual.get(100));
                }
            }
        } finally {
            for (PaimonCatalog catalog : catalogs) {
                catalog.close();
            }
        }
    }

    private void compactWhilePaused(FileStoreTable table) throws Exception {
        assertTrue(table.coreOptions().writeOnly());
        long previousSnapshot = table.snapshotManager().latestSnapshotId();
        FileStoreTable compactionTable =
                table.copy(Collections.singletonMap("write-only", "false"));
        try (StreamTableWrite write = compactionTable.newWrite("independent-compaction");
                TableCommitImpl commit = compactionTable.newCommit("independent-compaction")) {
            for (Split split : compactionTable.newReadBuilder().newScan().plan().splits()) {
                DataSplit dataSplit = (DataSplit) split;
                write.compact(dataSplit.partition(), dataSplit.bucket(), true);
            }
            commit.filterAndCommit(Collections.singletonMap(1L, write.prepareCommit(true, 1L)));
        }
        assertTrue(table.snapshotManager().latestSnapshotId() > previousSnapshot);
        assertEquals(
                org.apache.paimon.Snapshot.CommitKind.COMPACT,
                table.snapshotManager().latestSnapshot().commitKind());
    }

    private void awaitRows(
            List<PaimonCatalog> catalogs,
            Map<TablePath, SeaTunnelSink> sinks,
            String expected,
            JobClient client)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        String lastObservation = "";
        while (System.nanoTime() < deadline) {
            if (client.getJobExecutionResult().isDone()) {
                client.getJobExecutionResult().get();
                throw new AssertionError("Streaming job ended before expected rows were committed");
            }
            boolean complete = true;
            int tableIndex = 0;
            for (TablePath path : sinks.keySet()) {
                FileStoreTable table =
                        (FileStoreTable) catalogs.get(tableIndex++).getPaimonTable(path);
                ReadBuilder read = table.newReadBuilder();
                boolean updated = false;
                boolean deleted = true;
                int count = 0;
                try (RecordReader<InternalRow> reader =
                                read.newRead().createReader(read.newScan().plan());
                        RecordReaderIterator<InternalRow> iterator =
                                new RecordReaderIterator<>(reader)) {
                    while (iterator.hasNext()) {
                        InternalRow value = iterator.next();
                        count++;
                        if (value.getInt(0) == 99) {
                            deleted = false;
                        }
                        if (value.getInt(0) == 98) {
                            updated = expected.equals(value.getString(2).toString());
                        }
                    }
                }
                complete &= updated && deleted && count == 99;
                lastObservation =
                        "count="
                                + count
                                + ", updated="
                                + updated
                                + ", deleted="
                                + deleted
                                + ", snapshot="
                                + table.snapshotManager().latestSnapshotId();
            }
            if (complete) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                "Timed out waiting for committed Paimon update/delete: "
                        + expected
                        + "; "
                        + lastObservation);
    }

    private static final class SnapshotThenIncrementalSource
            extends RichParallelSourceFunction<SeaTunnelRow>
            implements CheckpointedFunction, CheckpointListener {

        private final List<SeaTunnelRow> rows;
        private final boolean bounded;
        private final boolean expectRestored;
        private final Map<Long, Integer> snapshotPhases =
                new java.util.concurrent.ConcurrentHashMap<>();
        private volatile boolean running = true;
        private volatile int phase;
        private volatile int checkpointedPhase;
        private volatile int completedAfterRestore;
        private transient ListState<Integer> state;
        private boolean restored;

        private SnapshotThenIncrementalSource(List<SeaTunnelRow> rows, boolean bounded) {
            this(rows, bounded, false);
        }

        private SnapshotThenIncrementalSource(
                List<SeaTunnelRow> rows, boolean bounded, boolean expectRestored) {
            this.rows = rows;
            this.bounded = bounded;
            this.expectRestored = expectRestored;
        }

        @Override
        public void run(SourceContext<SeaTunnelRow> context) throws Exception {
            if (restored) {
                phase = 3;
                while (running && completedAfterRestore < 3) {
                    Thread.sleep(10);
                }
                synchronized (context.getCheckpointLock()) {
                    if (getRuntimeContext().getIndexOfThisSubtask()
                            == getRuntimeContext().getNumberOfParallelSubtasks() - 1) {
                        for (SeaTunnelRow value : rows) {
                            if (value.getRowKind() == RowKind.UPDATE_AFTER
                                    && "after-3".equals(value.getField(2))) {
                                SeaTunnelRow resumed =
                                        new SeaTunnelRow(new Object[] {98, "part-0", "restored"});
                                resumed.setTableId(value.getTableId());
                                resumed.setRowKind(RowKind.UPDATE_AFTER);
                                context.collect(resumed);
                                SeaTunnelRow deleted =
                                        new SeaTunnelRow(new Object[] {97, "part-1", "before"});
                                deleted.setTableId(value.getTableId());
                                deleted.setRowKind(RowKind.DELETE);
                                context.collect(deleted);
                                SeaTunnelRow inserted =
                                        new SeaTunnelRow(
                                                new Object[] {100, "part-0", "new-after-restore"});
                                inserted.setTableId(value.getTableId());
                                inserted.setRowKind(RowKind.INSERT);
                                context.collect(inserted);
                            }
                        }
                    }
                    phase = 3;
                }
            } else {
                synchronized (context.getCheckpointLock()) {
                    if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                        for (SeaTunnelRow value : rows) {
                            if (value.getRowKind() == RowKind.INSERT) {
                                context.collect(value);
                            }
                        }
                    }
                    phase = 1;
                }
                while (running && checkpointedPhase < 1) {
                    Thread.sleep(10);
                }
                synchronized (context.getCheckpointLock()) {
                    if (getRuntimeContext().getIndexOfThisSubtask() == 1) {
                        for (SeaTunnelRow value : rows) {
                            if (value.getRowKind() != RowKind.INSERT) {
                                context.collect(value);
                            }
                        }
                    }
                    phase = 2;
                }
            }
            while (running && (!bounded || checkpointedPhase < 2)) {
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(phase);
            snapshotPhases.put(context.getCheckpointId(), phase);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("phase", Integer.class));
            restored = context.isRestored();
            if (expectRestored && !restored) {
                throw new IllegalStateException(
                        "Savepoint was not restored by the test environment");
            }
            if (restored) {
                for (Integer savedPhase : state.get()) {
                    phase = savedPhase;
                }
                if (phase < 2) {
                    throw new IllegalStateException(
                            "Test expects a savepoint after incremental writes");
                }
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            Integer savedPhase = snapshotPhases.get(checkpointId);
            if (savedPhase != null) {
                if (restored && savedPhase >= 3) {
                    completedAfterRestore++;
                }
                checkpointedPhase = Math.max(checkpointedPhase, savedPhase);
                snapshotPhases.keySet().removeIf(identifier -> identifier <= checkpointId);
            }
        }
    }

    private static SeaTunnelRow row(TablePath table, RowKind kind, int identifier, String value) {
        SeaTunnelRow row =
                new SeaTunnelRow(new Object[] {identifier, "part-" + identifier % 2, value});
        row.setTableId(table.toString());
        row.setRowKind(kind);
        return row;
    }

    private static final class TestPaimonSink extends PaimonSink {

        private TestPaimonSink(ReadonlyConfig config, CatalogTable table) {
            super(config, table);
        }

        @Override
        public Optional<SinkWriteRouting> getWriteRouting() {
            return Boolean.getBoolean("issue113.test.legacy-routing")
                    ? Optional.empty()
                    : super.getWriteRouting();
        }
    }
}

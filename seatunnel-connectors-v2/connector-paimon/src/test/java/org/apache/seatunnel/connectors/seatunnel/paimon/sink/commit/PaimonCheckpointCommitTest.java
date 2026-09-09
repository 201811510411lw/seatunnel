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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SinkWriteRouting;
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
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.state.PaimonSinkState;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonCheckpointCommitTest {

    private static final String CATALOG_NAME = "paimon_catalog";
    private static final String DATABASE_NAME = "checkpoint_test";
    private static final String TABLE_NAME = "cdc_table";

    @TempDir private Path temporaryDirectory;

    private PaimonCatalog catalog;
    private PaimonSink sink;
    private TablePath tablePath;
    private List<PaimonSinkWriter> writers;

    @BeforeEach
    void setUp() throws Exception {
        setUp(1);
    }

    private void setUp(int buckets) throws Exception {
        tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);

        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", temporaryDirectory.resolve("warehouse").toString());
        properties.put("plugin_name", "Paimon");
        properties.put("database", DATABASE_NAME);
        properties.put("table", TABLE_NAME);
        properties.put(
                "paimon.table.write-props",
                Collections.singletonMap("bucket", String.valueOf(buckets)));
        ReadonlyConfig config = ReadonlyConfig.fromMap(properties);

        catalog = new PaimonCatalog(CATALOG_NAME, config);
        catalog.open();
        catalog.createDatabase(tablePath, false);

        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "primary key"))
                        .column(
                                PhysicalColumn.of(
                                        "value",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        "value"))
                        .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                        .build();
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, TABLE_NAME),
                        schema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "checkpoint commit test");
        catalog.createTable(tablePath, table, false);

        JobContext jobContext = new JobContext(1L);
        jobContext.setJobMode(JobMode.STREAMING);
        jobContext.setEnableCheckpoint(true);
        sink = new PaimonSink(config, table);
        sink.setJobContext(jobContext);
        writers =
                Arrays.asList(
                        sink.createWriter(new DefaultSinkWriterContext(0, 2)),
                        sink.createWriter(new DefaultSinkWriterContext(1, 2)));
    }

    @Test
    void shouldDeleteSnapshotRowAfterWriterHandoff() throws Exception {
        enableRouting();
        PaimonAggregatedCommitter committer =
                (PaimonAggregatedCommitter) sink.createAggregatedCommitter().get();
        for (int identifier = 0; identifier < 100; identifier++) {
            writeRouted(row(RowKind.INSERT, identifier, "before"));
        }
        writeRouted(row(RowKind.DELETE, 99, "before"));
        commitCheckpoint(committer, 1L);
        assertFalse(readRows(currentTable()).containsKey(99));
    }

    @Test
    void shouldRetainLatestUpdateAndRestoreRoutedCheckpoint() throws Exception {
        enableRouting();
        PaimonAggregatedCommitter committer =
                (PaimonAggregatedCommitter) sink.createAggregatedCommitter().get();
        for (int identifier = 0; identifier < 100; identifier++) {
            writeRouted(row(RowKind.INSERT, identifier, "snapshot"));
        }
        commitCheckpoint(committer, 1L);
        writeRouted(row(RowKind.UPDATE_BEFORE, 98, "snapshot"));
        writeRouted(row(RowKind.UPDATE_AFTER, 98, "first"));
        writeRouted(row(RowKind.UPDATE_BEFORE, 98, "first"));
        writeRouted(row(RowKind.UPDATE_AFTER, 98, "latest"));
        writeRouted(row(RowKind.DELETE, 99, "snapshot"));
        List<PaimonAggregatedCommitInfo> fragments = prepareCheckpoint(committer, 2L);
        List<List<PaimonSinkState>> states = new ArrayList<>();
        for (PaimonSinkWriter writer : writers) {
            List<PaimonSinkState> state = writer.snapshotState(2L);
            assertEquals(2, state.get(0).getBucketRoutingVersion());
            states.add(
                    Collections.singletonList(
                            sink.getWriterStateSerializer()
                                    .get()
                                    .deserialize(
                                            sink.getWriterStateSerializer()
                                                    .get()
                                                    .serialize(state.get(0)))));
            writer.close();
        }
        committer.commit(fragments);
        writers = new ArrayList<>();
        for (int writerIndex = 0; writerIndex < 2; writerIndex++) {
            writers.add(
                    (PaimonSinkWriter)
                            sink.restoreWriter(
                                    new DefaultSinkWriterContext(writerIndex, 2),
                                    states.get(writerIndex)));
        }
        writeRouted(row(RowKind.UPDATE_AFTER, 98, "restored"));
        commitCheckpoint(committer, 3L);
        Map<Integer, String> actual = readRows(currentTable());
        assertEquals(99, actual.size());
        assertFalse(actual.containsKey(99));
        assertEquals("restored", actual.get(98));
        assertThrows(
                IllegalStateException.class,
                () -> sink.restoreWriter(new DefaultSinkWriterContext(0, 3), states.get(0)));
    }

    @ParameterizedTest
    @CsvSource({"1,write", "2,write", "2,prepare", "2,snapshot"})
    void shouldWaitForCompleteGlobalCommitWhenRestoringBothWriters(
            int routingVersion, String operation) throws Exception {
        tearDown();
        temporaryDirectory = temporaryDirectory.resolve("same-parallelism");
        setUp(4);
        enableRouting();
        for (int identifier = 0; identifier < 100; identifier++) {
            writeRouted(row(RowKind.INSERT, identifier, "before"));
        }
        PaimonAggregatedCommitter committer =
                (PaimonAggregatedCommitter) sink.createAggregatedCommitter().get();
        commitCheckpoint(committer, 0L);
        Map<Integer, String> expected = new LinkedHashMap<>();
        for (int identifier = 0; identifier < 100; identifier++) {
            String value = "pending-" + identifier;
            writeRouted(row(RowKind.UPDATE_AFTER, identifier, value));
            expected.put(identifier, value);
        }
        List<PaimonAggregatedCommitInfo> fragments = prepareCheckpoint(committer, 1L);
        List<List<PaimonSinkState>> states = snapshotAndCloseWriters(1L);
        long previousSnapshot = currentTable().snapshotManager().latestSnapshotId();
        for (int writerIndex = 0; writerIndex < 2; writerIndex++) {
            PaimonSinkState state = states.get(writerIndex).get(0);
            assertFalse(state.getCommitTables().isEmpty());
            state.setBucketRoutingVersion(routingVersion);
            writers.add(
                    (PaimonSinkWriter)
                            sink.restoreWriter(
                                    new DefaultSinkWriterContext(writerIndex, 2),
                                    states.get(writerIndex)));
            assertEquals(
                    previousSnapshot,
                    currentTable().snapshotManager().latestSnapshotId().longValue());
        }
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch started = new CountDownLatch(1);
            Future<?> waiting =
                    executor.submit(
                            () -> {
                                started.countDown();
                                accessRestoredWriter(operation, writers.get(0));
                                return null;
                            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> waiting.get(200, TimeUnit.MILLISECONDS));
            assertEquals(
                    previousSnapshot,
                    currentTable().snapshotManager().latestSnapshotId().longValue());
            committer.commit(fragments);
            waiting.get(5, TimeUnit.SECONDS);
            assertEquals(expected, readRows(currentTable()));
            for (int identifier = 0; identifier < 100; identifier++) {
                String value = "restored-" + identifier;
                writeRouted(row(RowKind.UPDATE_AFTER, identifier, value));
                expected.put(identifier, value);
            }
            commitCheckpoint(committer, 2L);
            commitCheckpoint(committer, 3L);
            commitCheckpoint(committer, 4L);
            assertEquals(expected, readRows(currentTable()));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"write", "prepare", "snapshot"})
    void shouldFailClosedWhenGlobalRecoveryDoesNotComplete(String operation) throws Exception {
        tearDown();
        temporaryDirectory = temporaryDirectory.resolve("recovery-timeout");
        setUp(4);
        enableRouting();
        for (int identifier = 0; identifier < 100; identifier++) {
            writeRouted(row(RowKind.INSERT, identifier, "pending-" + identifier));
        }
        PaimonAggregatedCommitter committer =
                (PaimonAggregatedCommitter) sink.createAggregatedCommitter().get();
        List<PaimonAggregatedCommitInfo> fragments = prepareCheckpoint(committer, 1L);
        List<List<PaimonSinkState>> states = snapshotAndCloseWriters(1L);
        sink.setLoadTable(
                currentTable().copy(Collections.singletonMap("commit.timeout", "100 ms")));
        PaimonSinkWriter restored =
                (PaimonSinkWriter)
                        sink.restoreWriter(new DefaultSinkWriterContext(0, 2), states.get(0));
        writers.add(restored);
        IOException failure =
                assertThrows(IOException.class, () -> accessRestoredWriter(operation, restored));
        assertTrue(failure.getMessage().contains("global commit"));
        assertTrue(readRows(currentTable()).isEmpty());
        committer.commit(fragments);
        accessRestoredWriter(operation, restored);
    }

    @Test
    void shouldRestoreIdleWriterWithoutWaitingForGlobalCommit() throws Exception {
        enableRouting();
        SeaTunnelRow value = row(RowKind.INSERT, 1, "pending");
        int owner = sink.getWriteRouting().get().route(value, 2);
        writeRouted(value);
        PaimonAggregatedCommitter committer =
                (PaimonAggregatedCommitter) sink.createAggregatedCommitter().get();
        prepareCheckpoint(committer, 1L);
        List<List<PaimonSinkState>> states = snapshotAndCloseWriters(1L);
        int idleWriter = 1 - owner;
        assertTrue(states.get(idleWriter).get(0).getCommitTables().isEmpty());
        sink.setLoadTable(
                currentTable().copy(Collections.singletonMap("commit.timeout", "100 ms")));
        PaimonSinkWriter restored =
                (PaimonSinkWriter)
                        sink.restoreWriter(
                                new DefaultSinkWriterContext(idleWriter, 2),
                                states.get(idleWriter));
        writers.add(restored);
        assertTrue(restored.prepareCommit(2L).get().getCommittables().isEmpty());
        assertTrue(restored.snapshotState(2L).get(0).getCommitTables().isEmpty());
        assertTrue(readRows(currentTable()).isEmpty());
    }

    private void accessRestoredWriter(String operation, PaimonSinkWriter writer) throws Exception {
        switch (operation) {
            case "write":
                SinkWriteRouting routing = sink.getWriteRouting().get();
                for (int identifier = 0; identifier < 100; identifier++) {
                    SeaTunnelRow value = row(RowKind.UPDATE_AFTER, identifier, "after-wait");
                    if (routing.route(value, 2) == 0) {
                        writer.write(value);
                        return;
                    }
                }
                throw new AssertionError("Expected a bucket owned by writer zero");
            case "prepare":
                writer.prepareCommit(2L);
                return;
            case "snapshot":
                writer.snapshotState(2L);
                return;
            default:
                throw new AssertionError("Unknown writer operation: " + operation);
        }
    }

    @Test
    void shouldRestorePreviouslyActiveWritersWithOnlyEmptyCommitMessages() throws Exception {
        tearDown();
        temporaryDirectory = temporaryDirectory.resolve("empty-recovery-boundary");
        setUp(4);
        enableRouting();
        for (int identifier = 0; identifier < 100; identifier++) {
            writeRouted(row(RowKind.INSERT, identifier, "before"));
        }
        PaimonAggregatedCommitter committer =
                (PaimonAggregatedCommitter) sink.createAggregatedCommitter().get();
        commitCheckpoint(committer, 1L);
        long committedSnapshot = currentTable().snapshotManager().latestSnapshotId();
        committer.commit(prepareCheckpoint(committer, 2L));
        List<List<PaimonSinkState>> states = snapshotAndCloseWriters(2L);
        assertEquals(
                committedSnapshot, currentTable().snapshotManager().latestSnapshotId().longValue());
        sink.setLoadTable(
                currentTable().copy(Collections.singletonMap("commit.timeout", "100 ms")));
        for (int writerIndex = 0; writerIndex < 2; writerIndex++) {
            assertFalse(states.get(writerIndex).get(0).getCommitTables().isEmpty());
            PaimonSinkWriter restored =
                    (PaimonSinkWriter)
                            sink.restoreWriter(
                                    new DefaultSinkWriterContext(writerIndex, 2),
                                    states.get(writerIndex));
            writers.add(restored);
            restored.prepareCommit(3L);
            restored.snapshotState(3L);
        }
        assertEquals(100, readRows(currentTable()).size());
    }

    private List<List<PaimonSinkState>> snapshotAndCloseWriters(long checkpointId)
            throws Exception {
        List<List<PaimonSinkState>> states = new ArrayList<>();
        for (PaimonSinkWriter writer : writers) {
            PaimonSinkState state = writer.snapshotState(checkpointId).get(0);
            states.add(
                    Collections.singletonList(
                            sink.getWriterStateSerializer()
                                    .get()
                                    .deserialize(
                                            sink.getWriterStateSerializer()
                                                    .get()
                                                    .serialize(state))));
            writer.close();
        }
        writers = new ArrayList<>();
        return states;
    }

    @ParameterizedTest
    @CsvSource({"1,false", "4,false", "1,true", "4,true"})
    void shouldRestoreBothWritersIntoOneWithoutLosingPendingCommits(
            int buckets, boolean compactBeforeRestore) throws Exception {
        tearDown();
        temporaryDirectory = temporaryDirectory.resolve("rescale");
        setUp(buckets);
        enableRouting();
        for (int identifier = 0; identifier < 100; identifier++) {
            writeRouted(row(RowKind.INSERT, identifier, "before"));
        }
        List<PaimonSinkState> states = new ArrayList<>();
        PaimonAggregatedCommitter committer =
                (PaimonAggregatedCommitter) sink.createAggregatedCommitter().get();
        commitCheckpoint(committer, 0L);
        Map<Integer, String> expected = new LinkedHashMap<>();
        for (int identifier = 0; identifier < 100; identifier++) {
            String value = "pending-" + identifier;
            writeRouted(row(RowKind.UPDATE_AFTER, identifier, value));
            expected.put(identifier, value);
        }
        List<PaimonAggregatedCommitInfo> fragments = prepareCheckpoint(committer, 1L);
        for (PaimonSinkWriter writer : writers) {
            for (PaimonSinkState state : writer.snapshotState(1L)) {
                states.add(
                        sink.getWriterStateSerializer()
                                .get()
                                .deserialize(
                                        sink.getWriterStateSerializer().get().serialize(state)));
            }
            writer.close();
        }
        writers = new ArrayList<>();
        assertEquals(
                buckets == 1 ? 1 : 2,
                states.stream().filter(state -> !state.getCommitTables().isEmpty()).count());
        Collections.reverse(states);
        Long compactedSnapshot = null;
        if (compactBeforeRestore) {
            committer.commit(fragments);
            FileStoreTable table = currentTable();
            try (StreamTableWrite write = table.newWrite("paused-compaction");
                    TableCommitImpl commit = table.newCommit("paused-compaction")) {
                for (Split split : table.newReadBuilder().newScan().plan().splits()) {
                    DataSplit dataSplit = (DataSplit) split;
                    write.compact(dataSplit.partition(), dataSplit.bucket(), true);
                }
                commit.filterAndCommit(Collections.singletonMap(1L, write.prepareCommit(true, 1L)));
            }
            assertEquals(
                    org.apache.paimon.Snapshot.CommitKind.COMPACT,
                    currentTable().snapshotManager().latestSnapshot().commitKind());
            compactedSnapshot = currentTable().snapshotManager().latestSnapshotId();
        }
        writers.add(
                (PaimonSinkWriter) sink.restoreWriter(new DefaultSinkWriterContext(0, 1), states));
        assertEquals(expected, readRows(currentTable()));
        long recoveredSnapshot = currentTable().snapshotManager().latestSnapshotId();
        if (compactedSnapshot != null) {
            assertEquals(compactedSnapshot.longValue(), recoveredSnapshot);
        }
        committer.commit(fragments);
        writers.get(0).close();
        writers.clear();
        writers.add(
                (PaimonSinkWriter) sink.restoreWriter(new DefaultSinkWriterContext(0, 1), states));
        assertEquals(
                recoveredSnapshot, currentTable().snapshotManager().latestSnapshotId().longValue());
        writeRouted(row(RowKind.UPDATE_AFTER, 1, "after"));
        writeRouted(row(RowKind.DELETE, 2, "before"));
        writeRouted(row(RowKind.INSERT, 100, "new"));
        expected.put(1, "after");
        expected.remove(2);
        expected.put(100, "new");
        commitCheckpoint(committer, 2L);
        commitCheckpoint(committer, 3L);
        commitCheckpoint(committer, 4L);
        Map<Integer, String> actual = readRows(currentTable());
        assertEquals(expected, actual);
        assertEquals("after", actual.get(1));
        assertEquals("new", actual.get(100));
        assertFalse(actual.containsKey(2));
        PaimonSinkState next = writers.get(0).snapshotState(4L).get(0);
        assertEquals(1, next.getWriterParallelism());
        assertEquals(0, next.getWriterIndex());
    }

    @Test
    void shouldRejectIncompleteDuplicateAndInconsistentRescaleState() throws Exception {
        enableRouting();
        PaimonSinkState first = writers.get(0).snapshotState(1L).get(0);
        PaimonSinkState second = writers.get(1).snapshotState(1L).get(0);
        DefaultSinkWriterContext target = new DefaultSinkWriterContext(0, 1);
        assertThrows(
                IllegalStateException.class,
                () -> sink.restoreWriter(target, Collections.singletonList(first)));
        assertThrows(
                IllegalStateException.class,
                () -> sink.restoreWriter(target, Arrays.asList(first, first)));
        second.setCheckpointId(2L);
        assertThrows(
                IllegalStateException.class,
                () -> sink.restoreWriter(target, Arrays.asList(first, second)));
        second.setCheckpointId(1L);
        second.setCommitUser("another-job");
        assertThrows(
                IllegalStateException.class,
                () -> sink.restoreWriter(target, Arrays.asList(first, second)));
        second.setCommitUser(first.getCommitUser());
        second.setWriterParallelism(3);
        assertThrows(
                IllegalStateException.class,
                () -> sink.restoreWriter(target, Arrays.asList(first, second)));
        second.setWriterParallelism(2);
        first.setBucketRoutingVersion(1);
        second.setBucketRoutingVersion(1);
        assertThrows(
                IllegalStateException.class,
                () -> sink.restoreWriter(target, Arrays.asList(first, second)));
    }

    @Test
    void shouldRejectLegacyStateAndWrongWriter() throws Exception {
        enableRouting();
        PaimonSinkState legacy = new PaimonSinkState(Collections.emptyList(), "legacy", 1L);
        assertThrows(
                IllegalStateException.class,
                () ->
                        sink.restoreWriter(
                                new DefaultSinkWriterContext(0, 2),
                                Collections.singletonList(legacy)));
        SeaTunnelRow value = row(RowKind.INSERT, 99, "before");
        int owner = sink.getWriteRouting().get().route(value, 2);
        assertThrows(IllegalStateException.class, () -> writers.get(1 - owner).write(value));
    }

    @Test
    void shouldRejectSchemaEventsBeforeRouting() {
        SinkWriteRouting routing = sink.getWriteRouting().get();
        SeaTunnelRow event = new SeaTunnelRow(new Object[0]);
        event.setOptions(Collections.singletonMap("schema_change_event", "synthetic"));
        assertThrows(UnsupportedOperationException.class, () -> routing.route(event, 2));
    }

    private void enableRouting() throws Exception {
        for (PaimonSinkWriter writer : writers) {
            writer.close();
        }
        sink.getWriteRouting();
        writers =
                Arrays.asList(
                        sink.createWriter(new DefaultSinkWriterContext(0, 2)),
                        sink.createWriter(new DefaultSinkWriterContext(1, 2)));
    }

    private void writeRouted(SeaTunnelRow row) throws Exception {
        writers.get(sink.getWriteRouting().get().route(row, writers.size())).write(row);
    }

    @Test
    void shouldCommitAllWriterFragmentsAcrossSequentialCheckpoints() throws Exception {
        PaimonAggregatedCommitter committer =
                (PaimonAggregatedCommitter)
                        sink.createAggregatedCommitter()
                                .orElseThrow(() -> new AssertionError("committer is required"));

        writers.get(0).write(row(RowKind.INSERT, 1, "before"));
        writers.get(1).write(row(RowKind.INSERT, 2, "delete-me"));
        commitCheckpoint(committer, 1L);

        FileStoreTable firstSnapshotTable = currentTable();
        long firstSnapshotId = firstSnapshotTable.snapshotManager().latestSnapshotId();
        assertEquals(mapOf(1, "before", 2, "delete-me"), readRows(firstSnapshotTable));

        writers.get(0).write(row(RowKind.UPDATE_BEFORE, 1, "before"));
        writers.get(0).write(row(RowKind.UPDATE_AFTER, 1, "after"));
        writers.get(1).write(row(RowKind.DELETE, 2, "delete-me"));
        commitCheckpoint(committer, 2L);

        FileStoreTable secondSnapshotTable = currentTable();
        assertTrue(secondSnapshotTable.snapshotManager().latestSnapshotId() > firstSnapshotId);
        assertEquals(Collections.singletonMap(1, "after"), readRows(secondSnapshotTable));
    }

    @Test
    void shouldCommitChangesWrittenAcrossPrepareAndSnapshotBoundary() throws Exception {
        PaimonAggregatedCommitter committer =
                (PaimonAggregatedCommitter)
                        sink.createAggregatedCommitter()
                                .orElseThrow(() -> new AssertionError("committer is required"));

        writers.get(0).write(row(RowKind.INSERT, 1, "before"));
        commitCheckpoint(committer, 1L);
        long firstSnapshotId = currentTable().snapshotManager().latestSnapshotId();

        List<PaimonAggregatedCommitInfo> emptyBoundary = prepareCheckpoint(committer, 2L);
        writers.get(0).write(row(RowKind.UPDATE_BEFORE, 1, "before"));
        writers.get(0).write(row(RowKind.UPDATE_AFTER, 1, "after"));
        writers.get(0).snapshotState(2L);
        writers.get(1).snapshotState(2L);
        committer.commit(emptyBoundary);

        commitCheckpoint(committer, 3L);
        FileStoreTable updatedTable = currentTable();
        assertTrue(updatedTable.snapshotManager().latestSnapshotId() > firstSnapshotId);
        assertEquals(Collections.singletonMap(1, "after"), readRows(updatedTable));
    }

    private void commitCheckpoint(PaimonAggregatedCommitter committer, long checkpointId)
            throws Exception {
        List<PaimonAggregatedCommitInfo> fragments = prepareCheckpoint(committer, checkpointId);
        for (PaimonSinkWriter writer : writers) {
            writer.snapshotState(checkpointId);
        }
        committer.commit(fragments);
    }

    private List<PaimonAggregatedCommitInfo> prepareCheckpoint(
            PaimonAggregatedCommitter committer, long checkpointId) throws Exception {
        List<PaimonAggregatedCommitInfo> fragments = new ArrayList<>();
        for (PaimonSinkWriter writer : writers) {
            Optional<PaimonCommitInfo> commitInfo = writer.prepareCommit(checkpointId);
            fragments.add(
                    committer.combine(
                            Collections.singletonList(
                                    commitInfo.orElseThrow(
                                            () -> new AssertionError("commit info is required")))));
        }
        return fragments;
    }

    private FileStoreTable currentTable() {
        return (FileStoreTable) catalog.getPaimonTable(tablePath);
    }

    private Map<Integer, String> readRows(FileStoreTable table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        Map<Integer, String> rows = new LinkedHashMap<>();
        try (RecordReader<InternalRow> reader =
                        readBuilder.newRead().createReader(readBuilder.newScan().plan());
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                rows.put(row.getInt(0), row.getString(1).toString());
            }
        }
        return rows;
    }

    private SeaTunnelRow row(RowKind kind, int id, String value) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, value});
        row.setRowKind(kind);
        return row;
    }

    private Map<Integer, String> mapOf(int firstId, String first, int secondId, String second) {
        Map<Integer, String> values = new LinkedHashMap<>();
        values.put(firstId, first);
        values.put(secondId, second);
        return values;
    }

    @AfterEach
    void tearDown() throws Exception {
        for (PaimonSinkWriter writer : writers) {
            writer.close();
        }
        catalog.close();
    }
}

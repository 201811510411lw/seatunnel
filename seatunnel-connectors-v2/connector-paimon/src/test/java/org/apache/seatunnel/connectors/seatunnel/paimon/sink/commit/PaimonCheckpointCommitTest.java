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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);

        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", temporaryDirectory.resolve("warehouse").toString());
        properties.put("plugin_name", "Paimon");
        properties.put("database", DATABASE_NAME);
        properties.put("table", TABLE_NAME);
        properties.put("paimon.table.write-props", Collections.singletonMap("bucket", "1"));
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
                                            () ->
                                                    new AssertionError(
                                                            "commit info is required")))));
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

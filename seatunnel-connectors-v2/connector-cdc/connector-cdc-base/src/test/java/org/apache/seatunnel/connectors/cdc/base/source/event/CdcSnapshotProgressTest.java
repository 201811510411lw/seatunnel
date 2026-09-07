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

package org.apache.seatunnel.connectors.cdc.base.source.event;

import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CdcSnapshotProgressTest {

    @Test
    void summarizesCompletedProcessingQueuedAndUnplannedTables() {
        TableId completed = TableId.parse("sales.completed");
        TableId processing = TableId.parse("sales.processing");
        TableId queued = TableId.parse("sales.queued");
        TableId unplanned = TableId.parse("sales.unplanned");
        SnapshotSplit completedSplit = split("completed-0", completed);
        SnapshotSplit processingDone = split("processing-0", processing);
        SnapshotSplit processingActive = split("processing-1", processing);
        SnapshotSplit queuedSplit = split("queued-0", queued);
        Map<String, SnapshotSplit> assigned = new HashMap<>();
        assigned.put(completedSplit.splitId(), completedSplit);
        assigned.put(processingDone.splitId(), processingDone);
        assigned.put(processingActive.splitId(), processingActive);
        Map<String, SnapshotSplitWatermark> completedOffsets = new HashMap<>();
        completedOffsets.put(completedSplit.splitId(), null);
        completedOffsets.put(processingDone.splitId(), null);

        CdcSnapshotProgress progress =
                CdcSnapshotProgress.from(
                        new HashSet<>(Arrays.asList(completed, processing, queued, unplanned)),
                        Collections.singletonList(unplanned),
                        Arrays.asList(completed, processing, queued),
                        Collections.singletonList(queuedSplit),
                        assigned,
                        completedOffsets);

        assertFalse(progress.isSplitPlanningComplete());
        assertEquals(4, progress.getTotalTables());
        assertEquals(1, progress.getCompletedTables());
        assertEquals(1, progress.getProcessingTables());
        assertEquals(1, progress.getQueuedTables());
        assertEquals(1, progress.getWaitingForSplitPlanningTables());
        assertEquals(2, progress.getWaitingTables());
        assertEquals(
                CdcTableProgressStatus.WAITING_FOR_SPLIT_PLANNING,
                progress.table("sales.unplanned").getStatus());
        assertNull(progress.table("sales.unplanned").getTotalSplits());
        assertEquals(2, progress.table("sales.processing").getTotalSplits());
        assertEquals(1, progress.table("sales.processing").getCompletedSplits());
        assertEquals(4, progress.getTotalSplits());
        assertTrue(progress.isSplitProgressAvailable());
        assertEquals(2, progress.getCompletedSplits());
        assertEquals(1, progress.getAssignedSplits());
        assertEquals(1, progress.getWaitingSplits());
        assertEquals(
                CdcSplitProgressStatus.COMPLETED,
                progress.table("sales.processing").split("processing-0").getStatus());
        assertEquals(
                CdcSplitProgressStatus.ASSIGNED,
                progress.table("sales.processing").split("processing-1").getStatus());
        assertEquals(
                CdcSplitProgressStatus.QUEUED,
                progress.table("sales.queued").split("queued-0").getStatus());
    }

    @Test
    void boundsTableDetailsWithoutChangingSummaryTotals() {
        ArrayList<TableId> tables = new ArrayList<>();
        for (int index = 0; index < 201; index++) {
            tables.add(TableId.parse("sales.table_" + index));
        }

        CdcSnapshotProgress progress =
                CdcSnapshotProgress.from(
                        tables,
                        tables,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        String json = new CdcProgressEvent(CdcProgressPhase.SNAPSHOT, progress).toJson();

        assertEquals(201, progress.getTotalTables());
        assertEquals(200, progress.getTables().size());
        assertTrue(progress.isTablesTruncated());
        assertTrue(json.contains("\"tablesTruncated\":true"));
        assertTrue(json.contains("\"totalTables\":201"));
    }

    @Test
    void boundsSplitDetailsWithoutChangingSummaryTotals() {
        TableId table = TableId.parse("sales.large_table");
        ArrayList<SnapshotSplit> splits = new ArrayList<>();
        for (int index = 0; index < 501; index++) {
            splits.add(split("large-" + index, table));
        }

        CdcSnapshotProgress progress =
                CdcSnapshotProgress.from(
                        Collections.singletonList(table),
                        Collections.emptyList(),
                        Collections.singletonList(table),
                        splits,
                        Collections.emptyMap(),
                        Collections.emptyMap());
        String json = new CdcProgressEvent(CdcProgressPhase.SNAPSHOT, progress).toJson();

        assertEquals(501, progress.getTotalSplits());
        assertEquals(501, progress.getWaitingSplits());
        assertEquals(500, progress.table("sales.large_table").getSplits().size());
        assertTrue(progress.isSplitsTruncated());
        assertTrue(json.contains("\"splitsTruncated\":true"));
        assertFalse(json.contains("splitKey"));
        assertFalse(json.contains("splitStart"));
        assertFalse(json.contains("splitEnd"));
    }

    @Test
    void marksContradictorySplitStateUnknownWithoutDoubleCounting() {
        TableId table = TableId.parse("sales.conflicting");
        SnapshotSplit split = split("conflicting-0", table);

        CdcSnapshotProgress progress =
                CdcSnapshotProgress.from(
                        Collections.singletonList(table),
                        Collections.emptyList(),
                        Collections.singletonList(table),
                        Collections.singletonList(split),
                        Collections.singletonMap(split.splitId(), split),
                        Collections.emptyMap());

        assertEquals(1, progress.getTotalSplits());
        assertEquals(1, progress.getUnknownSplits());
        assertEquals(
                CdcSplitProgressStatus.UNKNOWN,
                progress.table("sales.conflicting").split("conflicting-0").getStatus());
    }

    @Test
    void omitsSplitSummaryWhenCompletedTableSplitFactsWereCleared() {
        TableId completed = TableId.parse("sales.completed");

        CdcSnapshotProgress progress =
                CdcSnapshotProgress.from(
                        Collections.singletonList(completed),
                        Collections.emptyList(),
                        Collections.singletonList(completed),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        String json = new CdcProgressEvent(CdcProgressPhase.INCREMENTAL, progress).toJson();

        assertFalse(progress.isSplitProgressAvailable());
        assertTrue(json.contains("\"splitProgressAvailable\":false"));
        assertFalse(json.contains("\"splitSummary\""));
        assertTrue(json.contains("\"splitsTruncated\":false"));
    }

    @Test
    void reportsTruncatedReliableDetailsWhenGlobalSplitSummaryIsUnavailable() {
        TableId cleared = TableId.parse("sales.cleared");
        TableId visible = TableId.parse("sales.visible");
        ArrayList<SnapshotSplit> visibleSplits = new ArrayList<>();
        for (int index = 0; index < 501; index++) {
            visibleSplits.add(split("visible-" + index, visible));
        }

        CdcSnapshotProgress progress =
                CdcSnapshotProgress.from(
                        Arrays.asList(cleared, visible),
                        Collections.emptyList(),
                        Arrays.asList(cleared, visible),
                        visibleSplits,
                        Collections.emptyMap(),
                        Collections.emptyMap());
        String json = new CdcProgressEvent(CdcProgressPhase.INCREMENTAL, progress).toJson();

        assertFalse(progress.isSplitProgressAvailable());
        assertTrue(progress.isSplitsTruncated());
        assertEquals(500, progress.table("sales.visible").getSplits().size());
        assertFalse(json.contains("\"splitSummary\""));
        assertTrue(json.contains("\"splitsTruncated\":true"));
    }

    @Test
    void escapesAllJsonControlCharactersInTableNames() {
        TableId table = new TableId("catalog", "sales", "line\n\t\b\f\r\u0001\"\\item");
        CdcSnapshotProgress progress =
                CdcSnapshotProgress.from(
                        Collections.singletonList(table),
                        Collections.singletonList(table),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap());

        String json = new CdcProgressEvent(CdcProgressPhase.SNAPSHOT, progress).toJson();

        assertTrue(json.contains("line\\n\\t\\b\\f\\r\\u0001\\\"\\\\item"));
        assertFalse(json.contains("line\n"));
    }

    private SnapshotSplit split(String id, TableId tableId) {
        return new SnapshotSplit(id, tableId, null, null, null);
    }
}

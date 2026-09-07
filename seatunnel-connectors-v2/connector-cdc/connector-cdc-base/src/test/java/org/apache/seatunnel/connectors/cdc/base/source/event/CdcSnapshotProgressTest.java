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

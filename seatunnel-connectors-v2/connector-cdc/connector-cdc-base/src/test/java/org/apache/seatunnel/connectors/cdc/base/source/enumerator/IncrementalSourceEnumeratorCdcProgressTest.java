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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.CdcProgressEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CdcProgressPhase;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotPhaseEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IncrementalSourceEnumeratorCdcProgressTest {

    @Test
    void publishesCurrentPhaseOnlyToReaderZeroAndResendsAfterRegistration() {
        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<SourceSplitBase> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        SplitAssigner splitAssigner = Mockito.mock(SplitAssigner.class);
        Mockito.when(context.registeredReaders()).thenReturn(new HashSet<>(Arrays.asList(0, 1)));
        Mockito.when(splitAssigner.getCdcProgress())
                .thenReturn(new CdcProgressEvent(CdcProgressPhase.SNAPSHOT_WAITING_CHECKPOINT));
        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(context, splitAssigner);

        enumerator.registerReader(1);
        enumerator.registerReader(0);

        ArgumentCaptor<SourceEvent> event = ArgumentCaptor.forClass(SourceEvent.class);
        Mockito.verify(context, Mockito.times(2))
                .sendEventToSourceReader(Mockito.eq(0), event.capture());
        Mockito.verify(context, Mockito.never())
                .sendEventToSourceReader(Mockito.eq(1), Mockito.any(SourceEvent.class));
        for (SourceEvent value : event.getAllValues()) {
            assertEquals(
                    CdcProgressPhase.SNAPSHOT_WAITING_CHECKPOINT,
                    ((CdcProgressEvent) value).getPhase());
        }
    }

    @Test
    void outwardProgressStopsPublishingSplitSummaryAfterAssignerCleanup() {
        TableId table1 = TableId.parse("db1.table1");
        TableId table2 = TableId.parse("db1.table2");
        Map<String, SnapshotSplit> assignedSplits =
                Stream.of(
                                split("db1.table1.1", table1),
                                split("db1.table1.2", table1),
                                split("db1.table2.1", table2),
                                split("db1.table2.2", table2))
                        .collect(Collectors.toMap(SnapshotSplit::splitId, value -> value));
        Map<String, SnapshotSplitWatermark> completedOffsets =
                assignedSplits.keySet().stream()
                        .map(
                                splitId ->
                                        new AbstractMap.SimpleEntry<>(
                                                splitId,
                                                new SnapshotSplitWatermark(null, null, null)))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        SnapshotPhaseState snapshotPhaseState =
                new SnapshotPhaseState(
                        Arrays.asList(table1, table2),
                        Collections.emptyList(),
                        assignedSplits,
                        completedOffsets,
                        true,
                        Collections.emptyList(),
                        false,
                        false);
        HybridPendingSplitsState checkpointState =
                new HybridPendingSplitsState(snapshotPhaseState, null);
        SplitAssigner.Context assignerContext =
                new SplitAssigner.Context<>(
                        null, Collections.emptySet(), assignedSplits, completedOffsets);
        HybridSplitAssigner splitAssigner =
                new HybridSplitAssigner<>(assignerContext, 1, 1, checkpointState, null, null);
        splitAssigner.getIncrementalSplitAssigner().setSplitAssigned(true);

        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<SourceSplitBase> runtimeContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(runtimeContext.registeredReaders()).thenReturn(Collections.singleton(0));
        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(runtimeContext, splitAssigner);

        enumerator.registerReader(0);
        enumerator.handleSourceEvent(
                0, new CompletedSnapshotPhaseEvent(Collections.singletonList(table1)));

        ArgumentCaptor<SourceEvent> event = ArgumentCaptor.forClass(SourceEvent.class);
        Mockito.verify(runtimeContext, Mockito.times(2))
                .sendEventToSourceReader(Mockito.eq(0), event.capture());
        String beforeCleanup = ((CdcProgressEvent) event.getAllValues().get(0)).toJson();
        String afterCleanup = ((CdcProgressEvent) event.getAllValues().get(1)).toJson();
        assertTrue(beforeCleanup.contains("\"splitProgressAvailable\":true"));
        assertTrue(beforeCleanup.contains("\"totalSplits\":4"));
        assertTrue(afterCleanup.contains("\"splitProgressAvailable\":false"));
        assertFalse(afterCleanup.contains("\"splitSummary\""));
    }

    private static SnapshotSplit split(String id, TableId tableId) {
        return new SnapshotSplit(id, tableId, null, null, null);
    }
}

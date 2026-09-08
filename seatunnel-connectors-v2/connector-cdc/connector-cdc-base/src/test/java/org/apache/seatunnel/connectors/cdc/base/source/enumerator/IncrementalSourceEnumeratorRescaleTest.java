package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotPhaseEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.IncrementalSplitReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceReader;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceSplitReader;
import org.apache.seatunnel.connectors.cdc.base.source.split.CompletedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsBySplits;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IncrementalSourceEnumeratorRescaleTest {

    @Test
    void twoReaderSavepointRestoresBothIncrementalSplitsIntoOneReader() throws Exception {
        List<TableId> tables =
                Arrays.asList(TableId.parse("sales.orders"), TableId.parse("sales.items"));
        SnapshotPhaseState snapshot =
                new SnapshotPhaseState(
                        tables,
                        Collections.emptyList(),
                        new HashMap<>(),
                        new HashMap<>(),
                        true,
                        Collections.emptyList(),
                        false,
                        false);
        SourceSplitEnumerator.Context<SourceSplitBase> originalContext = runtimeContext(2);
        IncrementalSourceEnumerator original =
                restoredEnumerator(originalContext, snapshot, tables);
        IncrementalSourceReader<Object, SourceConfig> first = reader(original, 0);
        IncrementalSourceReader<Object, SourceConfig> second = reader(original, 1);
        Offset firstOffset = Mockito.mock(Offset.class);
        Offset secondOffset = Mockito.mock(Offset.class);
        List<SourceSplitBase> savedSplits = new ArrayList<>();
        HybridPendingSplitsState savedEnumerator;
        try {
            first.addSplits(
                    Collections.singletonList(
                            new IncrementalSplit(
                                    "incremental-split-0",
                                    Collections.singletonList(tables.get(0)),
                                    firstOffset,
                                    null,
                                    new ArrayList<>())));
            second.addSplits(
                    Collections.singletonList(
                            new IncrementalSplit(
                                    "incremental-split-1",
                                    Collections.singletonList(tables.get(1)),
                                    secondOffset,
                                    null,
                                    new ArrayList<>())));
            savedSplits.addAll(first.snapshotState(7));
            savedSplits.addAll(second.snapshotState(7));
            savedEnumerator = (HybridPendingSplitsState) original.snapshotState(7);
        } finally {
            first.close();
            second.close();
        }
        SourceSplitEnumerator.Context<SourceSplitBase> restoredContext = runtimeContext(1);
        IncrementalSourceEnumerator restored =
                restoredEnumerator(
                        restoredContext, savedEnumerator.getSnapshotPhaseState(), tables);
        IncrementalSourceReader<Object, SourceConfig> merged = reader(restored, 0);
        try {
            restored.run();
            merged.addSplits(savedSplits);
            restored.handleSourceEvent(
                    0,
                    new IncrementalSplitReportEvent(
                            Arrays.asList(
                                    savedSplits.get(0).asIncrementalSplit(),
                                    savedSplits.get(1).asIncrementalSplit())));
            restored.handleSourceEvent(0, new CompletedSnapshotPhaseEvent(tables));
            restored.handleSplitRequest(0);
            for (long checkpointId = 8; checkpointId <= 10; checkpointId++) {
                List<SourceSplitBase> state = merged.snapshotState(checkpointId);
                assertEquals(2, state.size());
                for (SourceSplitBase split : state) {
                    IncrementalSplit incremental = split.asIncrementalSplit();
                    boolean isFirst = split.splitId().equals("incremental-split-0");
                    assertEquals(
                            Collections.singletonList(tables.get(isFirst ? 0 : 1)),
                            incremental.getTableIds());
                    assertSame(
                            isFirst ? firstOffset : secondOffset, incremental.getStartupOffset());
                }
                restored.snapshotState(checkpointId);
                restored.notifyCheckpointComplete(checkpointId);
            }
            Mockito.verify(restoredContext, Mockito.never())
                    .assignSplit(Mockito.anyInt(), Mockito.any(SourceSplitBase.class));
            Mockito.verify(restoredContext).signalNoMoreSplits(0);
        } finally {
            merged.close();
        }
    }

    private SourceSplitEnumerator.Context<SourceSplitBase> runtimeContext(int parallelism) {
        SourceSplitEnumerator.Context<SourceSplitBase> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.currentParallelism()).thenReturn(parallelism);
        Mockito.when(context.registeredReaders())
                .thenReturn(
                        parallelism == 1
                                ? Collections.singleton(0)
                                : new HashSet<>(Arrays.asList(0, 1)));
        return context;
    }

    private IncrementalSourceEnumerator restoredEnumerator(
            SourceSplitEnumerator.Context<SourceSplitBase> context,
            SnapshotPhaseState snapshot,
            List<TableId> tables) {
        SplitAssigner.Context assignerContext =
                new SplitAssigner.Context<>(
                        null,
                        new HashSet<>(tables),
                        snapshot.getAssignedSplits(),
                        snapshot.getSplitCompletedOffsets());
        return new IncrementalSourceEnumerator(
                context,
                new HybridSplitAssigner<>(
                        assignerContext,
                        context.currentParallelism(),
                        2,
                        new HybridPendingSplitsState(snapshot, new IncrementalPhaseState()),
                        null,
                        null));
    }

    private IncrementalSourceReader<Object, SourceConfig> reader(
            IncrementalSourceEnumerator enumerator, int readerId) throws Exception {
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getIndexOfSubtask()).thenReturn(readerId);
        Mockito.when(context.getMetricsContext()).thenReturn(Mockito.mock(MetricsContext.class));
        Mockito.doAnswer(
                        invocation -> {
                            enumerator.handleSourceEvent(readerId, invocation.getArgument(0));
                            return null;
                        })
                .when(context)
                .sendSourceEventToEnumerator(Mockito.any(SourceEvent.class));
        IncrementalSourceSplitReader<SourceConfig> splitReader =
                Mockito.mock(IncrementalSourceSplitReader.class);
        Mockito.when(splitReader.fetch())
                .thenReturn(new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet()));
        return new IncrementalSourceReader<>(
                Mockito.mock(DataSourceDialect.class),
                new LinkedBlockingQueue<>(1),
                () -> splitReader,
                Mockito.mock(RecordEmitter.class),
                Mockito.mock(SourceReaderOptions.class),
                context,
                Mockito.mock(SourceConfig.class),
                Mockito.mock(DebeziumDeserializationSchema.class));
    }

    @Test
    void returnedRestoredSplitIsReassignedWithoutChangingOffset() throws Exception {
        TableId table = TableId.parse("sales.orders");
        SplitAssigner.Context assignerContext =
                new SplitAssigner.Context<>(
                        null, Collections.singleton(table), new HashMap<>(), new HashMap<>());
        IncrementalSplitAssigner assigner =
                new IncrementalSplitAssigner<>(assignerContext, 1, null);
        SourceSplitEnumerator.Context<SourceSplitBase> runtimeContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(runtimeContext.currentParallelism()).thenReturn(1);
        Mockito.when(runtimeContext.registeredReaders()).thenReturn(Collections.singleton(0));
        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(runtimeContext, assigner);
        IncrementalSplit restored =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(table),
                        Mockito.mock(Offset.class),
                        null,
                        new ArrayList<>());
        enumerator.handleSourceEvent(
                0, new IncrementalSplitReportEvent(Collections.singletonList(restored)));
        enumerator.run();
        enumerator.addSplitsBack(Collections.singletonList(restored), 0);
        enumerator.handleSourceEvent(0, new IncrementalSplitReportEvent(Collections.emptyList()));
        enumerator.handleSplitRequest(0);
        Mockito.verify(runtimeContext).assignSplit(0, restored);
        Mockito.verify(runtimeContext, Mockito.never()).signalNoMoreSplits(0);
    }

    @Test
    void scaleUpDoesNotReassignReaderOwnedTablesBeforeAllReadersReport() throws Exception {
        TableId table = TableId.parse("sales.orders");
        SnapshotPhaseState snapshot =
                new SnapshotPhaseState(
                        Collections.singletonList(table),
                        Collections.emptyList(),
                        new HashMap<>(),
                        new HashMap<>(),
                        true,
                        Collections.emptyList(),
                        false,
                        false);
        SplitAssigner.Context assignerContext =
                new SplitAssigner.Context<>(
                        null, Collections.singleton(table), new HashMap<>(), new HashMap<>());
        HybridSplitAssigner assigner =
                new HybridSplitAssigner<>(
                        assignerContext,
                        2,
                        1,
                        new HybridPendingSplitsState(snapshot, new IncrementalPhaseState()),
                        null,
                        null);
        SourceSplitEnumerator.Context<SourceSplitBase> runtimeContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(runtimeContext.currentParallelism()).thenReturn(2);
        Mockito.when(runtimeContext.registeredReaders())
                .thenReturn(new java.util.HashSet<>(java.util.Arrays.asList(0, 1)));
        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(runtimeContext, assigner);
        enumerator.handleSourceEvent(1, new IncrementalSplitReportEvent(Collections.emptyList()));
        enumerator.handleSplitRequest(1);
        assertDoesNotThrow(enumerator::run);
        Mockito.verify(runtimeContext, Mockito.never())
                .assignSplit(Mockito.anyInt(), Mockito.any(SourceSplitBase.class));

        IncrementalSplit restored =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(table),
                        Mockito.mock(Offset.class),
                        null,
                        new ArrayList<>());
        enumerator.handleSourceEvent(
                0, new IncrementalSplitReportEvent(Collections.singletonList(restored)));

        Mockito.verify(runtimeContext, Mockito.never())
                .assignSplit(Mockito.anyInt(), Mockito.any(SourceSplitBase.class));
        Mockito.verify(runtimeContext).signalNoMoreSplits(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void restoredReaderCanReportCompletedSnapshotBeforeRequestingAnotherSplit(int parallelism)
            throws Exception {
        TableId table = TableId.parse("sales.orders");
        SnapshotPhaseState snapshot =
                new SnapshotPhaseState(
                        Collections.singletonList(table),
                        Collections.emptyList(),
                        new HashMap<>(),
                        new HashMap<>(),
                        true,
                        Collections.emptyList(),
                        false,
                        false);
        SplitAssigner.Context assignerContext =
                new SplitAssigner.Context<>(
                        null, Collections.singleton(table), new HashMap<>(), new HashMap<>());
        HybridSplitAssigner assigner =
                new HybridSplitAssigner<>(
                        assignerContext,
                        parallelism,
                        1,
                        new HybridPendingSplitsState(snapshot, new IncrementalPhaseState()),
                        null,
                        null);
        SourceSplitEnumerator.Context<SourceSplitBase> runtimeContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(runtimeContext.registeredReaders()).thenReturn(Collections.singleton(0));
        Mockito.when(runtimeContext.currentParallelism()).thenReturn(parallelism);
        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(runtimeContext, assigner);

        enumerator.registerReader(0);

        IncrementalSourceReader<Object, SourceConfig> reader = reader(enumerator, 0);
        Offset offset = Mockito.mock(Offset.class);
        CompletedSnapshotSplitInfo completed =
                new CompletedSnapshotSplitInfo(
                        "sales.orders.0",
                        table,
                        null,
                        null,
                        null,
                        new SnapshotSplitWatermark("sales.orders.0", offset, offset));
        IncrementalSplit restored =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(table),
                        offset,
                        null,
                        new ArrayList<>(Collections.singletonList(completed)));
        try {
            assertDoesNotThrow(() -> reader.addSplits(Collections.singletonList(restored)));
            assertDoesNotThrow(
                    () ->
                            enumerator.handleSourceEvent(
                                    0,
                                    new CompletedSnapshotPhaseEvent(
                                            Collections.singletonList(table))));
            IncrementalSplit readerState = reader.snapshotState(9).get(0).asIncrementalSplit();
            assertEquals(Collections.singletonList(table), readerState.getTableIds());
            assertSame(offset, readerState.getStartupOffset());
            assertEquals(Collections.emptyList(), readerState.getCompletedSnapshotSplitInfos());
            Mockito.verify(runtimeContext, Mockito.never())
                    .assignSplit(Mockito.anyInt(), Mockito.any(SourceSplitBase.class));
        } finally {
            reader.close();
        }
    }

    @Test
    void completionFromUnreportedTableIsRejected() {
        SplitAssigner.Context assignerContext =
                new SplitAssigner.Context<>(
                        null,
                        Collections.singleton(TableId.parse("sales.orders")),
                        new HashMap<>(),
                        new HashMap<>());
        IncrementalSplitAssigner assigner =
                new IncrementalSplitAssigner<>(assignerContext, 1, null);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        assigner.completedSnapshotPhase(
                                Collections.singletonList(TableId.parse("sales.orders"))));
    }

    @Test
    void snapshotCompletionStillRequiresCheckpointConfirmation() {
        TableId table = TableId.parse("sales.orders");
        SnapshotPhaseState snapshot =
                new SnapshotPhaseState(
                        Collections.singletonList(table),
                        Collections.emptyList(),
                        new HashMap<>(),
                        new HashMap<>(),
                        false,
                        Collections.emptyList(),
                        false,
                        false);
        SplitAssigner.Context assignerContext =
                new SplitAssigner.Context<>(
                        null, Collections.singleton(table), new HashMap<>(), new HashMap<>());
        HybridSplitAssigner assigner =
                new HybridSplitAssigner<>(
                        assignerContext,
                        1,
                        1,
                        new HybridPendingSplitsState(snapshot, new IncrementalPhaseState()),
                        null,
                        null);
        SourceSplitEnumerator.Context<SourceSplitBase> runtimeContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(runtimeContext.registeredReaders()).thenReturn(Collections.singleton(0));
        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(runtimeContext, assigner);
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(table),
                        null,
                        null,
                        Collections.emptyList());
        enumerator.handleSourceEvent(
                0, new IncrementalSplitReportEvent(Collections.singletonList(split)));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        enumerator.handleSourceEvent(
                                0,
                                new CompletedSnapshotPhaseEvent(Collections.singletonList(table))));
    }
}

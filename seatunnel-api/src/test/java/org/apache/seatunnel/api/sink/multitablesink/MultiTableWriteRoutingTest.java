package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriteRouting;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class MultiTableWriteRoutingTest {

    @Test
    void shouldDelegateRoutingBySourceTable() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("database", "first"), sink("target-first", 0));
        sinks.put(TablePath.of("database", "second"), sink("target-second", 1));
        SinkWriteRouting routing = multiTable(sinks, 1).getWriteRouting().get();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId("database.first");
        assertEquals(0, routing.route(row, 2));
        row.setTableId("database.second");
        assertEquals(1, routing.route(row, 2));
        row.setTableId("database.missing");
        assertThrows(IllegalArgumentException.class, () -> routing.route(row, 2));
    }

    @Test
    void shouldRejectReplicaAndDuplicatePhysicalTarget() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("database", "first"), sink("same-target", 0));
        assertThrows(
                UnsupportedOperationException.class, () -> multiTable(sinks, 2).getWriteRouting());
        sinks.put(TablePath.of("database", "second"), sink("same-target", 0));
        assertThrows(IllegalArgumentException.class, () -> multiTable(sinks, 1).getWriteRouting());
    }

    @Test
    void shouldRejectMissingStateForRoutedWriter() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("database", "first"), sink("target", 0));
        MultiTableSink multiTable = multiTable(sinks, 1);
        multiTable.getWriteRouting();
        assertThrows(
                IllegalStateException.class,
                () ->
                        multiTable.restoreWriter(
                                new org.apache.seatunnel.api.sink.DefaultSinkWriterContext(0, 2),
                                Collections.singletonList(
                                        new MultiTableState(Collections.emptyMap()))));
    }

    @Test
    void shouldMergeAllOldWriterStatesWhenRestoringToOneWriter() throws Exception {
        TablePath table = TablePath.of("database", "first");
        SeaTunnelSink delegate = sink("target", 0);
        when(delegate.restoreWriter(any(), any()))
                .thenReturn(
                        mock(
                                SinkWriter.class,
                                withSettings().extraInterfaces(SupportMultiTableSinkWriter.class)));
        MultiTableSink multiTable = multiTable(Collections.singletonMap(table, delegate), 1);
        multiTable.getWriteRouting();
        multiTable.restoreWriter(
                new org.apache.seatunnel.api.sink.DefaultSinkWriterContext(0, 1),
                Arrays.asList(
                        new MultiTableState(
                                Collections.singletonMap(
                                        SinkIdentifier.of(table.toString(), 0),
                                        Collections.singletonList("writer-zero"))),
                        new MultiTableState(
                                Collections.singletonMap(
                                        SinkIdentifier.of(table.toString(), 1),
                                        Collections.singletonList("writer-one")))));
        verify(delegate).restoreWriter(any(), eq(Arrays.asList("writer-zero", "writer-one")));
    }

    @Test
    void shouldRejectRemovedTableEvenWithoutRescaling() throws Exception {
        TablePath table = TablePath.of("database", "first");
        SeaTunnelSink delegate = sink("target", 0);
        when(delegate.restoreWriter(any(), any()))
                .thenReturn(
                        mock(
                                SinkWriter.class,
                                withSettings().extraInterfaces(SupportMultiTableSinkWriter.class)));
        MultiTableSink multiTable = multiTable(Collections.singletonMap(table, delegate), 1);
        multiTable.getWriteRouting();
        Map<SinkIdentifier, java.util.List<?>> oldTables = new HashMap<>();
        oldTables.put(SinkIdentifier.of(table.toString(), 0), Collections.singletonList("first"));
        oldTables.put(
                SinkIdentifier.of("database.removed", 0), Collections.singletonList("removed"));
        assertThrows(
                IllegalStateException.class,
                () ->
                        multiTable.restoreWriter(
                                new org.apache.seatunnel.api.sink.DefaultSinkWriterContext(0, 2),
                                Collections.singletonList(new MultiTableState(oldTables))));
    }

    @Test
    void shouldPreserveNonRoutedSinksAndRejectMixedRouting() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        SeaTunnelSink plain = mock(SeaTunnelSink.class);
        when(plain.getWriteRouting()).thenReturn(Optional.empty());
        sinks.put(TablePath.of("database", "first"), plain);
        assertTrue(!multiTable(sinks, 2).getWriteRouting().isPresent());
        sinks.put(TablePath.of("database", "second"), sink("target", 0));
        assertThrows(
                UnsupportedOperationException.class, () -> multiTable(sinks, 1).getWriteRouting());
    }

    private MultiTableSink multiTable(Map<TablePath, SeaTunnelSink> sinks, int replicas) {
        return new MultiTableSink(
                new MultiTableFactoryContext(
                        ReadonlyConfig.fromMap(
                                Collections.singletonMap("multi_table_sink_replica", replicas)),
                        getClass().getClassLoader(),
                        sinks));
    }

    private SeaTunnelSink sink(String target, int owner) {
        SeaTunnelSink sink = mock(SeaTunnelSink.class);
        SinkWriteRouting routing =
                new SinkWriteRouting() {
                    @Override
                    public int route(SeaTunnelRow row, int numberOfWriters) {
                        return owner;
                    }

                    @Override
                    public String targetIdentifier() {
                        return target;
                    }
                };
        when(sink.getWriteRouting()).thenReturn(Optional.of(routing));
        return sink;
    }
}

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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriteRouting;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSinkGlobalCommitRecovery;
import org.apache.seatunnel.api.sink.SupportSinkWriteRouting;
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
import static org.mockito.ArgumentMatchers.anyInt;
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
        SinkWriteRouting<SeaTunnelRow> routing =
                SupportSinkWriteRouting.resolve(multiTable(sinks, 1), 2).get();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId("database.first");
        assertEquals(0, routing.route(row));
        row.setTableId("database.second");
        assertEquals(1, routing.route(row));
        row.setTableId("database.missing");
        assertThrows(IllegalArgumentException.class, () -> routing.route(row));
    }

    @Test
    void shouldRejectReplicaAndDuplicatePhysicalTarget() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("database", "first"), sink("same-target", 0));
        assertThrows(
                UnsupportedOperationException.class,
                () -> SupportSinkWriteRouting.resolve(multiTable(sinks, 2), 2));
        sinks.put(TablePath.of("database", "second"), sink("same-target", 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> SupportSinkWriteRouting.resolve(multiTable(sinks, 1), 2));
    }

    @Test
    void shouldRejectMissingStateForGlobalCommitRecoveryWriter() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("database", "first"), recoverySink(false));
        MultiTableSink multiTable = multiTable(sinks, 1);
        IllegalStateException failure =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                multiTable.restoreWriter(
                                        new org.apache.seatunnel.api.sink.DefaultSinkWriterContext(
                                                0, 2),
                                        Collections.singletonList(
                                                new MultiTableState(Collections.emptyMap()))));
        assertTrue(failure.getMessage().contains("missingTables=[database.first]"));
        assertTrue(failure.getMessage().contains("writer=0"));
    }

    @Test
    void shouldMergeAllOldWriterStatesWhenRestoringToOneWriter() throws Exception {
        TablePath table = TablePath.of("database", "first");
        SeaTunnelSink delegate = recoverySink(false);
        when(delegate.restoreWriter(any(), any()))
                .thenReturn(
                        mock(
                                SinkWriter.class,
                                withSettings().extraInterfaces(SupportMultiTableSinkWriter.class)));
        MultiTableSink multiTable = multiTable(Collections.singletonMap(table, delegate), 1);
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
        SeaTunnelSink delegate = recoverySink(false);
        when(delegate.restoreWriter(any(), any()))
                .thenReturn(
                        mock(
                                SinkWriter.class,
                                withSettings().extraInterfaces(SupportMultiTableSinkWriter.class)));
        MultiTableSink multiTable = multiTable(Collections.singletonMap(table, delegate), 1);
        Map<SinkIdentifier, java.util.List<?>> oldTables = new HashMap<>();
        oldTables.put(SinkIdentifier.of(table.toString(), 0), Collections.singletonList("first"));
        oldTables.put(
                SinkIdentifier.of("database.removed", 0), Collections.singletonList("removed"));
        IllegalStateException failure =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                multiTable.restoreWriter(
                                        new org.apache.seatunnel.api.sink.DefaultSinkWriterContext(
                                                0, 2),
                                        Collections.singletonList(new MultiTableState(oldTables))));
        assertTrue(failure.getMessage().contains("unexpectedTables=[database.removed]"));
        assertTrue(failure.getMessage().contains("expectedStates=1, actualStates=2"));
    }

    @Test
    void shouldKeepUnroutedSinksAndRejectMixedRecoveryContracts() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        SeaTunnelSink plain = mock(SeaTunnelSink.class);
        sinks.put(TablePath.of("database", "first"), plain);
        assertTrue(!SupportSinkWriteRouting.resolve(multiTable(sinks, 2), 2).isPresent());
        sinks.put(TablePath.of("database", "second"), sink("target", 0));
        UnsupportedOperationException failure =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> SupportSinkWriteRouting.resolve(multiTable(sinks, 1), 2));
        assertTrue(failure.getMessage().contains("Cannot mix routed and unrouted sinks"));
    }

    @Test
    void shouldKeepRoutingAndGlobalCommitRecoveryIndependent() throws Exception {
        TablePath routedTable = TablePath.of("database", "routed");
        SeaTunnelSink routedSink = sink("target", 0);
        when(routedSink.createWriter(any()))
                .thenReturn(
                        mock(
                                SinkWriter.class,
                                withSettings().extraInterfaces(SupportMultiTableSinkWriter.class)));
        MultiTableSink routedMultiTable =
                multiTable(Collections.singletonMap(routedTable, routedSink), 1);
        assertTrue(SupportSinkWriteRouting.resolve(routedMultiTable, 1).isPresent());
        assertTrue(!routedMultiTable.requiresGlobalCommitRecovery());
        routedMultiTable.restoreWriter(
                new org.apache.seatunnel.api.sink.DefaultSinkWriterContext(0, 1),
                Collections.singletonList(new MultiTableState(Collections.emptyMap())));
        verify(routedSink).createWriter(any());

        TablePath recoveryTable = TablePath.of("database", "recovery");
        MultiTableSink recoveryMultiTable =
                multiTable(Collections.singletonMap(recoveryTable, recoverySink(false)), 1);
        assertTrue(!SupportSinkWriteRouting.resolve(recoveryMultiTable, 1).isPresent());
        assertTrue(recoveryMultiTable.requiresGlobalCommitRecovery());
    }

    @Test
    void shouldRejectMixedGlobalCommitRecoveryContracts() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("database", "first"), recoverySink(false));
        sinks.put(TablePath.of("database", "second"), mock(SeaTunnelSink.class));
        UnsupportedOperationException failure =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> multiTable(sinks, 1).requiresGlobalCommitRecovery());
        assertTrue(
                failure.getMessage()
                        .contains("Cannot mix sinks with and without global commit recovery"));
    }

    @Test
    void shouldRejectReplicaForGlobalCommitRecovery() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("database", "first"), recoverySink(false));
        UnsupportedOperationException failure =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> multiTable(sinks, 2).requiresGlobalCommitRecovery());
        assertTrue(failure.getMessage().contains("multi_table_sink_replica = 1"));
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
        SeaTunnelSink sink =
                mock(
                        SeaTunnelSink.class,
                        withSettings().extraInterfaces(SupportSinkWriteRouting.class));
        SinkWriteRouting<SeaTunnelRow> routing =
                new SinkWriteRouting<SeaTunnelRow>() {
                    @Override
                    public int route(SeaTunnelRow row) {
                        return owner;
                    }

                    @Override
                    public String targetIdentifier() {
                        return target;
                    }
                };
        when(((SupportSinkWriteRouting<SeaTunnelRow>) sink).getWriteRouting(anyInt()))
                .thenReturn(Optional.of(routing));
        return sink;
    }

    private SeaTunnelSink recoverySink(boolean withRouting) {
        SeaTunnelSink sink =
                withRouting
                        ? mock(
                                SeaTunnelSink.class,
                                withSettings()
                                        .extraInterfaces(
                                                SupportSinkWriteRouting.class,
                                                SupportSinkGlobalCommitRecovery.class))
                        : mock(
                                SeaTunnelSink.class,
                                withSettings()
                                        .extraInterfaces(SupportSinkGlobalCommitRecovery.class));
        when(((SupportSinkGlobalCommitRecovery) sink).requiresGlobalCommitRecovery())
                .thenReturn(true);
        if (withRouting) {
            when(((SupportSinkWriteRouting<SeaTunnelRow>) sink).getWriteRouting(anyInt()))
                    .thenReturn(Optional.empty());
        }
        return sink;
    }
}

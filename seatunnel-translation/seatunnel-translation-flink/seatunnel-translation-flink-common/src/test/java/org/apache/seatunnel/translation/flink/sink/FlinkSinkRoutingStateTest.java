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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriteRouting;
import org.apache.seatunnel.api.sink.SupportSinkGlobalCommitRecovery;
import org.apache.seatunnel.api.sink.SupportSinkWriteRouting;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.Sink.ProcessingTimeService;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class FlinkSinkRoutingStateTest {

    @Test
    void shouldCommitRecoveredRoutedGlobalMessagesBeforeFiltering() throws Exception {
        SeaTunnelSink delegate = recoverySink();
        SinkAggregatedCommitter aggregated = mock(SinkAggregatedCommitter.class);
        when(delegate.createAggregatedCommitter()).thenReturn(Optional.of(aggregated));
        when(aggregated.restoreCommit(Collections.singletonList("complete-boundary")))
                .thenReturn(Collections.emptyList());
        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 2);
        try (GlobalCommitter committer = (GlobalCommitter) sink.createGlobalCommitter().get()) {
            // Paimon chooses whether to confirm empty recovery snapshots when it constructs its
            // aggregated committer, so the starter must have initialized routing before this path.
            InOrder initialization = inOrder(delegate);
            initialization
                    .verify((SupportSinkGlobalCommitRecovery) delegate)
                    .requiresGlobalCommitRecovery();
            initialization.verify(delegate).createAggregatedCommitter();
            assertTrue(
                    committer
                            .filterRecoveredCommittables(
                                    Collections.singletonList("complete-boundary"))
                            .isEmpty());
            verify(aggregated).restoreCommit(Collections.singletonList("complete-boundary"));
            verify(aggregated, never()).commit(any());
        }
    }

    @Test
    void shouldPreserveUnroutedGlobalRecoveryBehavior() throws Exception {
        SeaTunnelSink delegate = mock(SeaTunnelSink.class);
        SinkAggregatedCommitter aggregated = mock(SinkAggregatedCommitter.class);
        when(delegate.createAggregatedCommitter()).thenReturn(Optional.of(aggregated));
        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 2);
        try (GlobalCommitter committer = (GlobalCommitter) sink.createGlobalCommitter().get()) {
            assertTrue(
                    committer
                            .filterRecoveredCommittables(Collections.singletonList("legacy"))
                            .isEmpty());
            verify(aggregated, never()).commit(any());
            verify(aggregated, never()).restoreCommit(any());
        }
    }

    @Test
    void shouldPreserveRecoveryBehaviorWhenRoutingCapabilityIsEmpty() throws Exception {
        SeaTunnelSink delegate =
                mock(
                        SeaTunnelSink.class,
                        withSettings().extraInterfaces(SupportSinkWriteRouting.class));
        when(((SupportSinkWriteRouting) delegate).getWriteRouting(anyInt()))
                .thenReturn(Optional.empty());
        when(delegate.getWriterStateSerializer())
                .thenReturn(Optional.of(new DefaultSerializer<>()));
        when(delegate.getAggregatedCommitInfoSerializer())
                .thenReturn(Optional.of(new DefaultSerializer<>()));
        SinkAggregatedCommitter aggregated = mock(SinkAggregatedCommitter.class);
        when(delegate.createAggregatedCommitter()).thenReturn(Optional.of(aggregated));
        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 2);

        assertFalse(sink.createCommitter().isPresent());
        try (GlobalCommitter committer = (GlobalCommitter) sink.createGlobalCommitter().get()) {
            assertTrue(
                    committer
                            .filterRecoveredCommittables(Collections.singletonList("legacy"))
                            .isEmpty());
            verify(aggregated, never()).restoreCommit(any());
            verify(aggregated, never()).commit(any());
        }
        verify((SupportSinkWriteRouting) delegate, never()).getWriteRouting(anyInt());
    }

    @Test
    void shouldRejectIncompleteRoutedGlobalRecovery() throws Exception {
        SinkAggregatedCommitter aggregated = mock(SinkAggregatedCommitter.class);
        when(aggregated.commit(Collections.singletonList("retry")))
                .thenReturn(Collections.singletonList("retry"));
        when(aggregated.restoreCommit(Collections.singletonList("retry")))
                .thenReturn(Collections.singletonList("retry"));
        try (GlobalCommitter committer = new FlinkGlobalCommitter(aggregated, true)) {
            assertThrows(
                    java.io.IOException.class,
                    () -> committer.commit(Collections.singletonList("retry")));
            assertThrows(
                    java.io.IOException.class,
                    () ->
                            committer.filterRecoveredCommittables(
                                    Collections.singletonList("retry")));
        }
    }

    @Test
    void shouldKeepGlobalOnlyRoutedSinkStatefulWithoutCommittingLocally() throws Exception {
        SeaTunnelSink delegate = recoverySink();
        when(delegate.getWriterStateSerializer())
                .thenReturn(Optional.of(new DefaultSerializer<>()));
        when(delegate.getCommitInfoSerializer()).thenReturn(Optional.of(new DefaultSerializer<>()));
        when(delegate.getAggregatedCommitInfoSerializer())
                .thenReturn(Optional.of(new DefaultSerializer<>()));
        when(delegate.createAggregatedCommitter())
                .thenReturn(Optional.of(mock(SinkAggregatedCommitter.class)));
        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 2);
        try (Committer committer = (Committer) sink.createCommitter().get()) {
            assertTrue(committer.commit(Collections.singletonList("fragment")).isEmpty());
        }
    }

    @Test
    void shouldRejectMergingDifferentCheckpointBoundaries() throws Exception {
        SeaTunnelSink delegate = recoverySink();
        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 1);
        assertThrows(
                IllegalStateException.class,
                () ->
                        sink.createWriter(
                                mock(Sink.InitContext.class),
                                Arrays.asList(
                                        new FlinkWriterState<>(1L, "first"),
                                        new FlinkWriterState<>(2L, "second"))));
        verify(delegate, never()).restoreWriter(any(), any());
    }

    @Test
    void shouldRejectMissingRoutedWriterStateAtRestoredCheckpoint() throws Exception {
        SeaTunnelSink delegate = recoverySink();
        Sink.InitContext context = mock(Sink.InitContext.class);
        when(context.getRestoredCheckpointId()).thenReturn(OptionalLong.of(7L));
        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 2);
        assertThrows(
                IllegalStateException.class,
                () -> sink.createWriter(context, Collections.emptyList()));
        verify(delegate, never()).createWriter(any());
        verify(delegate, never()).restoreWriter(any(), any());
    }

    @Test
    void shouldNotChangeUnroutedSinkCommitterContract() throws Exception {
        SeaTunnelSink delegate = mock(SeaTunnelSink.class);
        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 2);
        assertTrue(!sink.createCommitter().isPresent());
    }

    @Test
    void shouldNotTreatRoutingAsGlobalCommitRecovery() throws Exception {
        SeaTunnelSink delegate = routedSink();
        Sink.InitContext context = new TestInitContext(OptionalLong.of(7L));
        org.apache.seatunnel.api.sink.SinkWriter writer =
                mock(org.apache.seatunnel.api.sink.SinkWriter.class);
        when(delegate.createWriter(any())).thenReturn(writer);

        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 2);

        assertFalse(sink.requiresGlobalCommitRecovery());
        assertFalse(sink.createCommitter().isPresent());
        sink.createWriter(context, Collections.emptyList());
        verify(delegate).createWriter(any());
        verify((SupportSinkWriteRouting) delegate, never()).getWriteRouting(anyInt());
    }

    @Test
    void shouldFailClosedWhenRequiredRecoveryHasNoAggregatedCommitter() throws Exception {
        SeaTunnelSink delegate = recoverySink();
        when(delegate.getWriterStateSerializer())
                .thenReturn(Optional.of(new DefaultSerializer<>()));
        when(delegate.getCommitInfoSerializer()).thenReturn(Optional.of(new DefaultSerializer<>()));
        when(delegate.getAggregatedCommitInfoSerializer())
                .thenReturn(Optional.of(new DefaultSerializer<>()));

        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 2);

        assertThrows(IllegalStateException.class, sink::createCommitter);
        assertThrows(IllegalStateException.class, sink::createGlobalCommitter);
    }

    @Test
    void shouldFailClosedWhenRequiredRecoverySerializersAreMissing() throws Exception {
        SeaTunnelSink missingWriterSerializer = recoverySink();
        assertThrows(
                IllegalStateException.class,
                () ->
                        new FlinkSink(missingWriterSerializer, Collections.emptyList(), 2)
                                .createCommitter());

        SeaTunnelSink missingCommitSerializer = recoverySink();
        when(missingCommitSerializer.getWriterStateSerializer())
                .thenReturn(Optional.of(new DefaultSerializer<>()));
        assertThrows(
                IllegalStateException.class,
                () ->
                        new FlinkSink(missingCommitSerializer, Collections.emptyList(), 2)
                                .createCommitter());

        SeaTunnelSink missingGlobalSerializer = recoverySink();
        when(missingGlobalSerializer.getWriterStateSerializer())
                .thenReturn(Optional.of(new DefaultSerializer<>()));
        when(missingGlobalSerializer.getCommitInfoSerializer())
                .thenReturn(Optional.of(new DefaultSerializer<>()));
        assertThrows(
                IllegalStateException.class,
                () ->
                        new FlinkSink(missingGlobalSerializer, Collections.emptyList(), 2)
                                .createCommitter());
    }

    private SeaTunnelSink routedSink() {
        SeaTunnelSink sink =
                mock(
                        SeaTunnelSink.class,
                        withSettings().extraInterfaces(SupportSinkWriteRouting.class));
        when(((SupportSinkWriteRouting) sink).getWriteRouting(anyInt()))
                .thenReturn(Optional.of(mock(SinkWriteRouting.class)));
        return sink;
    }

    private SeaTunnelSink recoverySink() {
        SeaTunnelSink sink =
                mock(
                        SeaTunnelSink.class,
                        withSettings().extraInterfaces(SupportSinkGlobalCommitRecovery.class));
        when(((SupportSinkGlobalCommitRecovery) sink).requiresGlobalCommitRecovery())
                .thenReturn(true);
        return sink;
    }

    private static final class TestInitContext implements Sink.InitContext {

        private final ContextWrapper context;
        private final OptionalLong restoredCheckpointId;

        private TestInitContext(OptionalLong restoredCheckpointId) {
            StreamingRuntimeContext runtimeContext = mock(StreamingRuntimeContext.class);
            OperatorMetricGroup metricGroup = mock(OperatorMetricGroup.class);
            when(runtimeContext.getJobId()).thenReturn(new JobID());
            when(runtimeContext.getLongCounter(anyString()))
                    .thenAnswer(ignored -> new LongCounter());
            when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
            when(metricGroup.meter(anyString(), any(org.apache.flink.metrics.Meter.class)))
                    .thenAnswer(invocation -> invocation.getArgument(1));
            this.context = new ContextWrapper(runtimeContext);
            this.restoredCheckpointId = restoredCheckpointId;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return mock(UserCodeClassLoader.class);
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return mock(MailboxExecutor.class);
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return mock(ProcessingTimeService.class);
        }

        @Override
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 2;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return mock(SinkWriterMetricGroup.class);
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return restoredCheckpointId;
        }
    }

    private static final class ContextWrapper {

        private final StreamingRuntimeContext runtimeContext;

        private ContextWrapper(StreamingRuntimeContext runtimeContext) {
            this.runtimeContext = runtimeContext;
        }
    }
}

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriteRouting;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlinkSinkRoutingStateTest {

    @Test
    void shouldCommitRecoveredRoutedGlobalMessagesBeforeFiltering() throws Exception {
        SeaTunnelSink delegate = mock(SeaTunnelSink.class);
        SinkAggregatedCommitter aggregated = mock(SinkAggregatedCommitter.class);
        when(delegate.getWriteRouting()).thenReturn(Optional.of(mock(SinkWriteRouting.class)));
        when(delegate.createAggregatedCommitter()).thenReturn(Optional.of(aggregated));
        when(aggregated.commit(Collections.singletonList("complete-boundary")))
                .thenReturn(Collections.emptyList());
        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 2);
        try (GlobalCommitter committer = (GlobalCommitter) sink.createGlobalCommitter().get()) {
            assertTrue(
                    committer
                            .filterRecoveredCommittables(
                                    Collections.singletonList("complete-boundary"))
                            .isEmpty());
            verify(aggregated).commit(Collections.singletonList("complete-boundary"));
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
        }
    }

    @Test
    void shouldRejectIncompleteRoutedGlobalRecovery() throws Exception {
        SinkAggregatedCommitter aggregated = mock(SinkAggregatedCommitter.class);
        when(aggregated.commit(Collections.singletonList("retry")))
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
        SeaTunnelSink delegate = mock(SeaTunnelSink.class);
        when(delegate.getWriteRouting()).thenReturn(Optional.of(mock(SinkWriteRouting.class)));
        when(delegate.getWriterStateSerializer())
                .thenReturn(Optional.of(new DefaultSerializer<>()));
        when(delegate.getAggregatedCommitInfoSerializer())
                .thenReturn(Optional.of(new DefaultSerializer<>()));
        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 2);
        try (Committer committer = (Committer) sink.createCommitter().get()) {
            assertTrue(committer.commit(Collections.singletonList("fragment")).isEmpty());
        }
    }

    @Test
    void shouldRejectMergingDifferentCheckpointBoundaries() throws Exception {
        SeaTunnelSink delegate = mock(SeaTunnelSink.class);
        when(delegate.getWriteRouting()).thenReturn(Optional.of(mock(SinkWriteRouting.class)));
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
    void shouldNotChangeUnroutedSinkCommitterContract() throws Exception {
        SeaTunnelSink delegate = mock(SeaTunnelSink.class);
        FlinkSink sink = new FlinkSink(delegate, Collections.emptyList(), 2);
        assertTrue(!sink.createCommitter().isPresent());
    }
}

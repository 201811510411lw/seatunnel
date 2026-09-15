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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FlinkRecoveryGlobalCommitterOperatorTest {

    @Test
    void shouldRejectMissingGlobalCheckpointStateBeforePublishingAnyBatch() throws Exception {
        Fixture fixture = new Fixture();

        IOException failure = assertThrows(IOException.class, () -> fixture.restore(7));

        assertTrue(failure.getMessage().contains("global checkpoint state"));
        assertTrue(failure.getMessage().contains("fresh snapshot"));
        assertTrue(fixture.committed.isEmpty());
        assertEquals(0, fixture.restoreCalls);
        assertEquals(0, fixture.normalCalls);
    }

    @Test
    void shouldRejectUnknownPreviousStateBeforePublishingAnyBatch() throws Exception {
        for (int version : new int[] {2, 3, 99}) {
            Fixture fixture = new Fixture();
            fixture.saved =
                    Collections.singletonList(
                            ByteBuffer.allocate(8).putInt(version).putInt(0).array());
            FlinkRuntimeException failure =
                    assertThrows(FlinkRuntimeException.class, () -> fixture.restore(7));
            assertTrue(failure.getCause() instanceof IOException);
            assertTrue(failure.getCause().getMessage().contains("fresh snapshot"));
            assertTrue(fixture.committed.isEmpty());
        }
    }

    @Test
    void shouldCommitLateRecoveredMessagesOnlyAfterEveryWriterIncludingEmptyWriters()
            throws Exception {
        Fixture fixture = new Fixture();
        fixture.snapshotEmptyState();
        FlinkRecoveryGlobalCommitterOperator<String, String> operator = fixture.restore(7);
        send(operator, summary(7, 0, 2, 1));
        send(operator, value(7, 0, "pending"));
        assertTrue(fixture.committed.isEmpty());
        send(operator, summary(7, 1, 2, 0));
        assertEquals(Collections.singletonList("pending"), fixture.committed);
        assertEquals(1, fixture.restoreCalls);
        assertEquals(0, fixture.normalCalls);
        operator.close();
    }

    @Test
    void shouldWaitForCheckpointCompletionBeforePublishingNewBatch() throws Exception {
        Fixture fixture = new Fixture();
        FlinkRecoveryGlobalCommitterOperator<String, String> operator = fixture.fresh();
        send(operator, summary(7, 0, 1, 1));
        send(operator, value(7, 0, "current"));
        operator.notifyCheckpointComplete(6);
        assertTrue(fixture.committed.isEmpty());
        operator.notifyCheckpointComplete(7);
        assertEquals(Collections.singletonList("current"), fixture.committed);
        assertEquals(1, fixture.normalCalls);
        assertEquals(0, fixture.restoreCalls);
        operator.close();
    }

    @Test
    void shouldPersistEmptyCompletedBoundaryWithoutPublishingPayload() throws Exception {
        Fixture fixture = new Fixture();
        FlinkRecoveryGlobalCommitterOperator<String, String> operator = fixture.fresh();
        send(operator, summary(7, 0, 2, 0));
        operator.notifyCheckpointComplete(7);
        send(operator, summary(7, 1, 2, 0));
        operator.snapshotState(mock(StateSnapshotContext.class));
        operator.close();

        FlinkRecoveryGlobalCommitterOperator<String, String> restored = fixture.restore(8);
        send(restored, summary(7, 0, 1, 1));
        send(restored, value(7, 0, "already-completed"));
        assertTrue(fixture.committed.isEmpty());
        assertEquals(0, fixture.normalCalls);
        assertEquals(0, fixture.restoreCalls);
        send(restored, summary(9, 0, 1, 1));
        send(restored, value(9, 0, "after-empty-boundary"));
        restored.notifyCheckpointComplete(9);
        assertEquals(Collections.singletonList("after-empty-boundary"), fixture.committed);
        restored.close();
    }

    @Test
    void shouldRequireCheckpointCompletionAndKeepOlderIncompleteBatchesAheadOfNewerOnes()
            throws Exception {
        Fixture fixture = new Fixture();
        FlinkRecoveryGlobalCommitterOperator<String, String> operator = fixture.fresh();
        send(operator, summary(7, 0, 2, 1));
        send(operator, value(7, 0, "older-a"));
        send(operator, summary(8, 0, 2, 1));
        send(operator, value(8, 0, "newer-a"));
        send(operator, summary(8, 1, 2, 1));
        send(operator, value(8, 1, "newer-b"));
        assertTrue(fixture.committed.isEmpty());
        operator.snapshotState(mock(StateSnapshotContext.class));
        operator.close();
        operator = fixture.restore(8);
        assertTrue(fixture.committed.isEmpty());
        send(operator, summary(7, 1, 2, 1));
        send(operator, value(7, 1, "older-b"));
        assertEquals(Arrays.asList("older-a,older-b", "newer-a,newer-b"), fixture.committed);
        operator.close();
    }

    @Test
    void shouldRetainPartialBatchAcrossAnotherRecoveryAndRememberCommittedFrontier()
            throws Exception {
        Fixture fixture = new Fixture();
        fixture.snapshotEmptyState();
        FlinkRecoveryGlobalCommitterOperator<String, String> first = fixture.restore(7);
        send(first, summary(7, 0, 2, 1));
        send(first, value(7, 0, "first"));
        first.snapshotState(mock(StateSnapshotContext.class));
        first.close();

        FlinkRecoveryGlobalCommitterOperator<String, String> second = fixture.restore(8);
        assertTrue(fixture.committed.isEmpty());
        send(second, summary(7, 1, 2, 1));
        send(second, value(7, 1, "second"));
        assertEquals(Collections.singletonList("first,second"), fixture.committed);
        second.snapshotState(mock(StateSnapshotContext.class));
        second.close();

        FlinkRecoveryGlobalCommitterOperator<String, String> third = fixture.restore(9);
        send(third, summary(7, 0, 2, 1));
        send(third, value(7, 0, "first"));
        assertEquals(Collections.singletonList("first,second"), fixture.committed);
        third.close();
    }

    @Test
    void shouldFailWithoutDiscardingCommittablesWhenGlobalCommitRequestsRetry() throws Exception {
        Fixture fixture = new Fixture();
        fixture.snapshotEmptyState();
        FlinkRecoveryGlobalCommitterOperator<String, String> operator = fixture.restore(7);
        send(operator, summary(7, 0, 1, 1));
        fixture.rejectCommit = true;
        assertThrows(IOException.class, () -> send(operator, value(7, 0, "retry")));
        operator.snapshotState(mock(StateSnapshotContext.class));
        operator.close();
        fixture.rejectCommit = false;
        FlinkRecoveryGlobalCommitterOperator<String, String> restored = fixture.restore(8);
        assertEquals(Collections.singletonList("retry"), fixture.committed);
        restored.close();
    }

    @Test
    void shouldCommitEndOfInputAndRejectAnIncompleteBoundedBatch() throws Exception {
        Fixture fixture = new Fixture();
        FlinkRecoveryGlobalCommitterOperator<String, String> operator = fixture.fresh();
        send(operator, summary(Long.MAX_VALUE, 0, 1, 1));
        send(operator, value(Long.MAX_VALUE, 0, "bounded"));
        assertTrue(fixture.committed.isEmpty());
        operator.endInput();
        assertEquals(Collections.singletonList("bounded"), fixture.committed);
        assertEquals(0, fixture.restoreCalls);
        assertEquals(1, fixture.normalCalls);
        operator.close();

        FlinkRecoveryGlobalCommitterOperator<String, String> incomplete = new Fixture().fresh();
        send(incomplete, summary(Long.MAX_VALUE, 0, 2, 0));
        assertThrows(IOException.class, incomplete::endInput);
        incomplete.close();
    }

    @Test
    void shouldRejectRestoringTerminalFrontierWithoutPublishingAgain() throws Exception {
        Fixture fixture = new Fixture();
        FlinkRecoveryGlobalCommitterOperator<String, String> operator = fixture.fresh();
        send(operator, summary(Long.MAX_VALUE, 0, 1, 1));
        send(operator, value(Long.MAX_VALUE, 0, "bounded"));
        operator.endInput();
        operator.snapshotState(mock(StateSnapshotContext.class));
        operator.close();

        IOException failure = assertThrows(IOException.class, () -> fixture.restore(7));
        assertTrue(failure.getMessage().contains("non-draining savepoint"));
        assertEquals(Collections.singletonList("bounded"), fixture.committed);
        assertEquals(1, fixture.normalCalls);
        assertEquals(0, fixture.restoreCalls);
    }

    @Test
    void shouldRejectPendingTerminalStateBeforePublishingRecoveredBatches() throws Exception {
        Fixture fixture = new Fixture();
        FlinkRecoveryGlobalCommitterOperator<String, String> operator = fixture.fresh();
        send(operator, summary(7, 0, 1, 1));
        send(operator, value(7, 0, "older"));
        send(operator, summary(Long.MAX_VALUE, 0, 1, 1));
        send(operator, value(Long.MAX_VALUE, 0, "bounded"));
        operator.snapshotState(mock(StateSnapshotContext.class));
        operator.close();

        IOException failure = assertThrows(IOException.class, () -> fixture.restore(8));
        assertTrue(failure.getMessage().contains("non-draining savepoint"));
        assertTrue(fixture.committed.isEmpty());
        assertEquals(0, fixture.normalCalls);
        assertEquals(0, fixture.restoreCalls);
    }

    private static CommittableSummary<String> summary(
            long checkpoint, int writer, int writers, int count) {
        return new CommittableSummary<>(writer, writers, checkpoint, count, 0, 0);
    }

    private static CommittableWithLineage<String> value(long checkpoint, int writer, String value) {
        return new CommittableWithLineage<>(value, checkpoint, writer);
    }

    private static void send(
            FlinkRecoveryGlobalCommitterOperator<String, String> operator,
            CommittableMessage<String> message)
            throws Exception {
        operator.processElement(new StreamRecord<>(message));
    }

    /**
     * Real state bytes are retained while Flink backend callbacks and external commits are stubbed.
     */
    private static final class Fixture {
        private final List<String> committed = new ArrayList<>();
        private List<byte[]> saved = Collections.emptyList();
        private boolean rejectCommit;
        private int restoreCalls;
        private int normalCalls;

        void snapshotEmptyState() throws Exception {
            FlinkRecoveryGlobalCommitterOperator<String, String> operator = fresh();
            try {
                operator.snapshotState(mock(StateSnapshotContext.class));
            } finally {
                operator.close();
            }
        }

        FlinkRecoveryGlobalCommitterOperator<String, String> fresh() throws Exception {
            return create(OptionalLong.empty());
        }

        FlinkRecoveryGlobalCommitterOperator<String, String> restore(long checkpoint)
                throws Exception {
            return create(OptionalLong.of(checkpoint));
        }

        private List<String> publish(List<String> batch, boolean restored) {
            if (restored) {
                restoreCalls++;
            } else {
                normalCalls++;
            }
            if (rejectCommit) {
                return batch;
            }
            committed.addAll(batch);
            return Collections.emptyList();
        }

        @SuppressWarnings("unchecked")
        private FlinkRecoveryGlobalCommitterOperator<String, String> create(OptionalLong checkpoint)
                throws Exception {
            GlobalCommitter<String, String> committer = mock(GlobalCommitter.class);
            when(committer.combine(any()))
                    .thenAnswer(
                            invocation ->
                                    String.join(",", invocation.<List<String>>getArgument(0)));
            when(committer.commit(any()))
                    .thenAnswer(invocation -> publish(invocation.getArgument(0), false));
            when(committer.filterRecoveredCommittables(any()))
                    .thenAnswer(invocation -> publish(invocation.getArgument(0), true));
            Sink<Void, String, Void, String> sink = mock(Sink.class);
            when(sink.createGlobalCommitter()).thenReturn(Optional.of(committer));
            when(sink.getCommittableSerializer())
                    .thenReturn(Optional.of(FlinkGlobalCommitStateSerializerTest.STRINGS));
            when(sink.getGlobalCommittableSerializer())
                    .thenReturn(Optional.of(FlinkGlobalCommitStateSerializerTest.STRINGS));
            ListState<byte[]> raw = mock(ListState.class);
            when(raw.get()).thenAnswer(ignored -> saved);
            doAnswer(
                            invocation -> {
                                saved = new ArrayList<>(invocation.getArgument(0));
                                return null;
                            })
                    .when(raw)
                    .update(any());
            OperatorStateStore store = mock(OperatorStateStore.class);
            when(store.getListState(any())).thenReturn((ListState) raw);
            StateInitializationContext context = mock(StateInitializationContext.class);
            when(context.getOperatorStateStore()).thenReturn(store);
            when(context.isRestored()).thenReturn(checkpoint.isPresent());
            when(context.getRestoredCheckpointId()).thenReturn(checkpoint);
            FlinkRecoveryGlobalCommitterOperator<String, String> operator =
                    new FlinkRecoveryGlobalCommitterOperator<>(sink);
            // Supply the callback target normally installed by Flink's runtime initialization,
            // following the same fixture setup as SchemaOperatorTest.
            Field stateHandler = AbstractStreamOperator.class.getDeclaredField("stateHandler");
            stateHandler.setAccessible(true);
            stateHandler.set(operator, mock(StreamOperatorStateHandler.class));
            operator.initializeState(context);
            return operator;
        }
    }
}

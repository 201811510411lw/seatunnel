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
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
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
    void shouldNeverTreatOneOldFragmentAsACompleteRescaledLegacyBatch() throws Exception {
        CommittableCollector<String> legacy = new CommittableCollector<>(0, 1);
        legacy.addMessage(summary(7, 0, 4, 1));
        legacy.addMessage(value(7, 0, "only-one-of-four"));
        byte[] bytes =
                FlinkGlobalCommitStateSerializerTest.legacyBytes(legacy, Collections.emptyList());
        for (int[] layout : new int[][] {{1, 0}, {1, 4}, {4, 4}}) {
            Fixture fixture = new Fixture();
            fixture.parallelism = layout[0];
            fixture.declaredLegacyWriters = layout[1];
            fixture.declaredLegacyCheckpoint = 7;
            fixture.saved = Collections.singletonList(versionedLegacy(bytes));
            FlinkRuntimeException failure =
                    assertThrows(FlinkRuntimeException.class, () -> fixture.restore(7));
            assertTrue(failure.getCause() instanceof IOException);
            assertTrue(fixture.committed.isEmpty());
        }
    }

    @Test
    void shouldBindLegacyLayoutDeclarationToTheActualRestoredCheckpoint() throws Exception {
        CommittableCollector<String> legacy = new CommittableCollector<>(0, 1);
        for (int writer = 0; writer < 4; writer++) {
            legacy.addMessage(summary(7, writer, 4, 1));
            legacy.addMessage(value(7, writer, "writer-" + writer));
        }
        Fixture fixture = new Fixture();
        fixture.parallelism = 4;
        fixture.declaredLegacyWriters = 4;
        fixture.declaredLegacyCheckpoint = 6;
        fixture.saved =
                Collections.singletonList(
                        versionedLegacy(
                                FlinkGlobalCommitStateSerializerTest.legacyBytes(
                                        legacy, Collections.emptyList())));
        FlinkRuntimeException failure =
                assertThrows(FlinkRuntimeException.class, () -> fixture.restore(7));
        assertTrue(failure.getCause() instanceof IOException);
        assertTrue(fixture.committed.isEmpty());
        fixture.declaredLegacyCheckpoint = 7;
        FlinkRecoveryGlobalCommitterOperator<String, String> restored = fixture.restore(7);
        assertEquals(
                Collections.singletonList("writer-0,writer-1,writer-2,writer-3"),
                fixture.committed);
        restored.close();
    }

    private static byte[] versionedLegacy(byte[] bytes) {
        return ByteBuffer.allocate(8 + bytes.length)
                .putInt(2)
                .putInt(bytes.length)
                .put(bytes)
                .array();
    }

    @Test
    void shouldCommitLateRecoveredMessagesOnlyAfterEveryWriterIncludingEmptyWriters()
            throws Exception {
        Fixture fixture = new Fixture();
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

    /** The state store serializes real bytes; only the external commit endpoint is replaced. */
    private static final class Fixture {
        private final List<String> committed = new ArrayList<>();
        private List<byte[]> saved = Collections.emptyList();
        private boolean rejectCommit;
        private int restoreCalls;
        private int normalCalls;
        private int parallelism = 2;
        private int declaredLegacyWriters;
        private long declaredLegacyCheckpoint = -1;

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
                    new FlinkRecoveryGlobalCommitterOperator<>(
                            sink, parallelism, declaredLegacyWriters, declaredLegacyCheckpoint);
            operator.initializeState(context);
            return operator;
        }
    }
}

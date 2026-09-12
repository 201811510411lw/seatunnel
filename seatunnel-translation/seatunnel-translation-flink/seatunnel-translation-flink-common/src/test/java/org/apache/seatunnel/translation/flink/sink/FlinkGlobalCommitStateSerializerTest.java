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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.operators.sink.committables.CheckpointCommittableManager;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollectorSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkGlobalCommitStateSerializerTest {

    static final SimpleVersionedSerializer<String> STRINGS =
            new SimpleVersionedSerializer<String>() {
                @Override
                public int getVersion() {
                    return 7;
                }

                @Override
                public byte[] serialize(String value) {
                    return value.getBytes(StandardCharsets.UTF_8);
                }

                @Override
                public String deserialize(int version, byte[] bytes) throws IOException {
                    if (version != 7) {
                        throw new IOException("Wrong payload serializer version");
                    }
                    return new String(bytes, StandardCharsets.UTF_8);
                }
            };

    @Test
    void shouldRejectLegacyPartialStateWithoutOriginalParallelismProof() throws Exception {
        CommittableCollector<String> collector = new CommittableCollector<>(0, 1);
        collector.addMessage(summary(0, 4, 11L, 1));
        collector.addMessage(new CommittableWithLineage<>("only-one-of-four", 11L, 0));
        byte[] bytes = legacyBytes(collector, Collections.emptyList());
        // The old format loses numberOfSubtasks=4. Candidate current parallelism=1 must not
        // become proof that this one surviving segment is the complete original producer set.
        FlinkGlobalCommitStateSerializer<String, String> withoutProof =
                new FlinkGlobalCommitStateSerializer<>(STRINGS, STRINGS);
        assertThrows(IOException.class, () -> withoutProof.deserialize(2, bytes));
        assertThrows(IOException.class, () -> serializer(4).deserialize(2, bytes));
    }

    @Test
    void shouldAllowOnlyEmptyLegacyStateWithoutProof() throws Exception {
        CommittableCollector<String> collector = new CommittableCollector<>(0, 1);
        FlinkGlobalCommitStateSerializer<String, String> withoutProof =
                new FlinkGlobalCommitStateSerializer<>(STRINGS, STRINGS);
        assertTrue(
                withoutProof
                        .deserialize(2, legacyBytes(collector, Collections.emptyList()))
                        .getBatches()
                        .isEmpty());
        assertThrows(
                IOException.class,
                () ->
                        withoutProof.deserialize(
                                2,
                                legacyBytes(
                                        collector, Collections.singletonList("old-aggregate"))));

        FlinkGlobalCommitState<String, String> current = new FlinkGlobalCommitState<>();
        current.addMessage(summary(0, 1, 12L, 0));
        assertTrue(
                withoutProof
                        .deserialize(3, withoutProof.serialize(current))
                        .getBatches()
                        .get(12L)
                        .isComplete());
    }

    @Test
    void shouldMigrateFlinkProducedCollectorWithoutLosingProducerSegments() throws Exception {
        CommittableCollector<String> collector = new CommittableCollector<>(0, 1);
        for (int writer : new int[] {3, 1, 0, 2}) {
            collector.addMessage(summary(writer, 4, 2422L, 1));
            collector.addMessage(new CommittableWithLineage<>("writer-" + writer, 2422L, writer));
        }
        FlinkGlobalCommitStateSerializer<String, String> serializer = serializer(4);
        FlinkGlobalCommitState<String, String> state =
                serializer.deserialize(2, legacyBytes(collector, Arrays.asList("old-a", "old-b")));
        assertEquals(Arrays.asList("old-a", "old-b"), state.getLegacyGlobalCommittables());
        assertEquals(-1L, state.getLastCommittedCheckpointId());
        FlinkGlobalCommitState.Batch<String> batch = state.getBatches().get(2422L);
        assertTrue(batch.isLegacy());
        assertTrue(batch.isComplete());
        assertEquals(4, batch.getExpectedWriters());
        assertEquals(4, batch.getCommittables().size());
        assertTrue(
                batch.getCommittables()
                        .containsAll(
                                Arrays.asList("writer-0", "writer-1", "writer-2", "writer-3")));

        FlinkGlobalCommitState<String, String> migrated =
                serializer(1).deserialize(3, serializer.serialize(state));
        assertTrue(migrated.getBatches().get(2422L).isComplete());
        assertEquals(4, migrated.getBatches().get(2422L).getExpectedWriters());
    }

    @Test
    void shouldRejectLegacyStateWithMissingProducerOrPayload() throws Exception {
        CommittableCollector<String> collector = new CommittableCollector<>(0, 1);
        collector.addMessage(summary(0, 2, 11L, 1));
        collector.addMessage(new CommittableWithLineage<>("first", 11L, 0));
        assertThrows(
                IOException.class,
                () ->
                        serializer(2)
                                .deserialize(2, legacyBytes(collector, Collections.emptyList())));
        collector.addMessage(summary(1, 2, 11L, 1));
        assertThrows(
                IOException.class,
                () ->
                        serializer(2)
                                .deserialize(2, legacyBytes(collector, Collections.emptyList())));
    }

    @Test
    void shouldPreserveIdlePaimonCommitWrapperAndRejectLegacyRescale() throws Exception {
        CommittableCollector<String> collector = new CommittableCollector<>(0, 1);
        collector.addMessage(summary(0, 2, 11L, 1));
        collector.addMessage(new CommittableWithLineage<>("active", 11L, 0));
        collector.addMessage(summary(1, 2, 11L, 1));
        collector.addMessage(new CommittableWithLineage<>("empty-paimon-data", 11L, 1));
        byte[] bytes = legacyBytes(collector, Collections.emptyList());
        assertTrue(serializer(2).deserialize(2, bytes).getBatches().get(11L).isComplete());
        assertThrows(IOException.class, () -> serializer(1).deserialize(2, bytes));
    }

    @Test
    void shouldMigrateFullyDrainedLegacyBatchBeforePendingBatch() throws Exception {
        CommittableCollector<String> collector = new CommittableCollector<>(0, 1);
        for (int writer = 0; writer < 4; writer++) {
            collector.addMessage(summary(writer, 4, 11L, 1));
            collector.addMessage(new CommittableWithLineage<>("committed-" + writer, 11L, writer));
        }
        for (CheckpointCommittableManager<String> batch :
                collector.getCheckpointCommittablesUpTo(11L)) {
            batch.commit(true, testCommitter(false));
        }
        for (int writer = 0; writer < 4; writer++) {
            collector.addMessage(summary(writer, 4, 12L, 1));
            collector.addMessage(new CommittableWithLineage<>("pending-" + writer, 12L, writer));
        }
        FlinkGlobalCommitState<String, String> restored =
                serializer(4).deserialize(2, legacyBytes(collector, Collections.emptyList()));
        assertEquals(11L, restored.getLastCommittedCheckpointId());
        assertEquals(Collections.singleton(12L), restored.getBatches().keySet());
        assertTrue(restored.getBatches().get(12L).isComplete());
        assertEquals(4, restored.getBatches().get(12L).getCommittables().size());
    }

    @Test
    void shouldRejectPartiallyDrainedLegacyBatch() throws Exception {
        CommittableCollector<String> collector = new CommittableCollector<>(0, 1);
        for (int writer = 0; writer < 2; writer++) {
            collector.addMessage(summary(writer, 2, 11L, 1));
            collector.addMessage(new CommittableWithLineage<>("writer-" + writer, 11L, writer));
        }
        for (CheckpointCommittableManager<String> batch :
                collector.getCheckpointCommittablesUpTo(11L)) {
            batch.commit(true, testCommitter(true));
        }
        assertThrows(
                IOException.class,
                () ->
                        serializer(2)
                                .deserialize(2, legacyBytes(collector, Collections.emptyList())));
    }

    @Test
    void shouldRejectLegacyProducerWithMultipleWrappers() throws Exception {
        CommittableCollector<String> collector = new CommittableCollector<>(0, 1);
        collector.addMessage(summary(0, 1, 11L, 2));
        collector.addMessage(new CommittableWithLineage<>("first", 11L, 0));
        collector.addMessage(new CommittableWithLineage<>("second", 11L, 0));
        assertThrows(
                IOException.class,
                () ->
                        serializer(1)
                                .deserialize(2, legacyBytes(collector, Collections.emptyList())));
    }

    @Test
    void shouldPreserveIncompleteNewBatchAcrossRecoveryAndParallelismChange() throws Exception {
        FlinkGlobalCommitState<String, String> state = new FlinkGlobalCommitState<>();
        for (int writer = 0; writer < 3; writer++) {
            state.addMessage(summary(writer, 4, 12L, 1));
            state.addMessage(new CommittableWithLineage<>("writer-" + writer, 12L, writer));
        }
        assertFalse(state.getBatches().get(12L).isComplete());
        FlinkGlobalCommitState<String, String> recovered =
                serializer(1).deserialize(3, serializer(4).serialize(state));
        assertFalse(recovered.getBatches().get(12L).isComplete());
        recovered.addMessage(new CommittableWithLineage<>("last", 12L, 3));
        assertFalse(recovered.getBatches().get(12L).isComplete());
        recovered.addMessage(summary(3, 4, 12L, 1));
        assertTrue(recovered.getBatches().get(12L).isComplete());
        assertEquals(4, recovered.getBatches().get(12L).getCommittables().size());
    }

    @Test
    void shouldPersistCommittedFrontierAndIgnoreReplay() throws Exception {
        FlinkGlobalCommitState<String, String> state = new FlinkGlobalCommitState<>();
        state.addMessage(summary(0, 1, 10L, 0));
        state.markCommitted(10L);
        FlinkGlobalCommitState<String, String> restored =
                SimpleVersionedSerialization.readVersionAndDeSerialize(
                        serializer(1),
                        SimpleVersionedSerialization.writeVersionAndSerialize(
                                serializer(1), state));
        restored.addMessage(summary(0, 1, 10L, 1));
        restored.addMessage(new CommittableWithLineage<>("already-committed", 10L, 0));
        assertTrue(restored.getBatches().isEmpty());
        assertEquals(10L, restored.getLastCommittedCheckpointId());
    }

    @Test
    void shouldHandleEndOfInputAndMessageBeforeSummary() throws Exception {
        FlinkGlobalCommitState<String, String> state = new FlinkGlobalCommitState<>();
        state.addMessage(new CommittableWithLineage<>("end", null, 0));
        FlinkGlobalCommitState<String, String> recovered =
                serializer(1).deserialize(3, serializer(1).serialize(state));
        assertFalse(recovered.getBatches().get(Long.MAX_VALUE).isComplete());
        recovered.addMessage(summary(0, 1, null, 1));
        assertTrue(recovered.getBatches().get(Long.MAX_VALUE).isComplete());
    }

    @Test
    void shouldRejectConflictingOrFailedSummariesAndExcessMessages() throws Exception {
        FlinkGlobalCommitState<String, String> state = new FlinkGlobalCommitState<>();
        state.addMessage(summary(0, 2, 1L, 1));
        state.addMessage(summary(0, 2, 1L, 1));
        assertFalse(state.getBatches().get(1L).isComplete());
        assertThrows(IOException.class, () -> state.addMessage(summary(0, 2, 1L, 2)));
        assertThrows(IOException.class, () -> state.addMessage(summary(1, 3, 1L, 1)));
        assertThrows(
                IOException.class,
                () -> state.addMessage(new CommittableSummary<>(1, 2, 1L, 1, 0, 1)));
        assertThrows(IOException.class, () -> state.addMessage(summary(1, 2, 1L, -1)));
        assertThrows(IOException.class, () -> state.addMessage(summary(-1, 2, 1L, 0)));
        state.addMessage(new CommittableWithLineage<>("value", 1L, 0));
        assertThrows(
                IOException.class,
                () -> state.addMessage(new CommittableWithLineage<>("duplicate", 1L, 0)));
    }

    @Test
    void shouldRejectCorruptLengthsVersionsAndTrailingBytes() throws Exception {
        byte[] valid = serializer(1).serialize(new FlinkGlobalCommitState<>());
        assertThrows(IOException.class, () -> serializer(1).deserialize(99, valid));
        assertThrows(
                IOException.class,
                () -> serializer(1).deserialize(3, Arrays.copyOf(valid, valid.length - 1)));
        assertThrows(
                IOException.class,
                () -> serializer(1).deserialize(3, Arrays.copyOf(valid, valid.length + 1)));
        byte[] corrupted = Arrays.copyOf(valid, valid.length);
        // Global payload list count follows magic, committed frontier and serializer version.
        Arrays.fill(corrupted, 16, 20, (byte) 0x7f);
        assertThrows(IOException.class, () -> serializer(1).deserialize(3, corrupted));
        Arrays.fill(corrupted, 16, 20, (byte) 0xff);
        assertThrows(IOException.class, () -> serializer(1).deserialize(3, corrupted));
    }

    private static CommittableSummary<String> summary(
            int writer, int parallelism, Long checkpoint, int count) {
        return new CommittableSummary<>(writer, parallelism, checkpoint, count, 0, 0);
    }

    private static Committer<String> testCommitter(boolean retrySecondWriter) {
        return new Committer<String>() {
            @Override
            public void commit(Collection<CommitRequest<String>> requests) {
                if (retrySecondWriter) {
                    requests.stream()
                            .filter(request -> request.getCommittable().equals("writer-1"))
                            .forEach(CommitRequest::retryLater);
                }
            }

            @Override
            public void close() {}
        };
    }

    private static FlinkGlobalCommitStateSerializer<String, String> serializer(int writers) {
        return new FlinkGlobalCommitStateSerializer<>(STRINGS, STRINGS, writers);
    }

    static byte[] legacyBytes(CommittableCollector<String> collector, List<String> aggregates)
            throws IOException {
        // Outer framing is Flink GlobalCommitterSerializer v2. Nested bytes are produced by Flink,
        // independently of the migration decoder; no real business checkpoint is a test fixture.
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(-1189141205);
        out.writeBoolean(true);
        SimpleVersionedSerialization.writeVersionAndSerializeList(STRINGS, aggregates, out);
        SimpleVersionedSerialization.writeVersionAndSerialize(
                new CommittableCollectorSerializer<>(STRINGS, 0, 1), collector, out);
        return out.getCopyOfBuffer();
    }
}

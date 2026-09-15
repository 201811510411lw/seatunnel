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

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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
    void shouldRejectOriginalFlinkAndUnknownStateFormats() throws Exception {
        byte[] valid = serializer().serialize(new FlinkGlobalCommitState<>());
        for (int version : new int[] {0, 2, 3, 99}) {
            IOException failure =
                    assertThrows(IOException.class, () -> serializer().deserialize(version, valid));
            assertTrue(failure.getMessage().contains("fresh snapshot"));
        }
        byte[] originalFlinkMagic = Arrays.copyOf(valid, valid.length);
        java.nio.ByteBuffer.wrap(originalFlinkMagic).putInt(-1189141205);
        IOException failure =
                assertThrows(
                        IOException.class, () -> serializer().deserialize(1, originalFlinkMagic));
        assertTrue(failure.getMessage().contains("fresh snapshot"));
    }

    @Test
    void shouldPreserveOriginalProducerSetAcrossRecovery() throws Exception {
        FlinkGlobalCommitState<String> state = new FlinkGlobalCommitState<>();
        for (int writer = 0; writer < 3; writer++) {
            state.addMessage(summary(writer, 4, 12L, 1));
            state.addMessage(new CommittableWithLineage<>("writer-" + writer, 12L, writer));
        }
        assertFalse(state.getBatches().get(12L).isComplete());
        FlinkGlobalCommitState<String> recovered =
                serializer().deserialize(1, serializer().serialize(state));
        assertFalse(recovered.getBatches().get(12L).isComplete());
        recovered.addMessage(new CommittableWithLineage<>("last", 12L, 3));
        assertFalse(recovered.getBatches().get(12L).isComplete());
        recovered.addMessage(summary(3, 4, 12L, 1));
        assertTrue(recovered.getBatches().get(12L).isComplete());
        assertEquals(4, recovered.getBatches().get(12L).getCommittables().size());
    }

    @Test
    void shouldPersistCommittedFrontierAndIgnoreReplay() throws Exception {
        FlinkGlobalCommitState<String> state = new FlinkGlobalCommitState<>();
        state.addMessage(summary(0, 1, 10L, 0));
        state.markCommitted(10L);
        FlinkGlobalCommitState<String> restored =
                SimpleVersionedSerialization.readVersionAndDeSerialize(
                        serializer(),
                        SimpleVersionedSerialization.writeVersionAndSerialize(serializer(), state));
        restored.addMessage(summary(0, 1, 10L, 1));
        restored.addMessage(new CommittableWithLineage<>("already-committed", 10L, 0));
        assertTrue(restored.getBatches().isEmpty());
        assertEquals(10L, restored.getLastCommittedCheckpointId());
    }

    @Test
    void shouldHandleEndOfInputAndMessageBeforeSummary() throws Exception {
        FlinkGlobalCommitState<String> state = new FlinkGlobalCommitState<>();
        state.addMessage(new CommittableWithLineage<>("end", null, 0));
        FlinkGlobalCommitState<String> recovered =
                serializer().deserialize(1, serializer().serialize(state));
        assertFalse(recovered.getBatches().get(Long.MAX_VALUE).isComplete());
        recovered.addMessage(summary(0, 1, null, 1));
        assertTrue(recovered.getBatches().get(Long.MAX_VALUE).isComplete());
    }

    @Test
    void shouldRejectConflictingOrFailedSummariesAndExcessMessages() throws Exception {
        FlinkGlobalCommitState<String> state = new FlinkGlobalCommitState<>();
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
        byte[] valid = serializer().serialize(new FlinkGlobalCommitState<>());
        assertThrows(IOException.class, () -> serializer().deserialize(99, valid));
        assertThrows(
                IOException.class,
                () -> serializer().deserialize(1, Arrays.copyOf(valid, valid.length - 1)));
        assertThrows(
                IOException.class,
                () -> serializer().deserialize(1, Arrays.copyOf(valid, valid.length + 1)));
        byte[] corrupted = Arrays.copyOf(valid, valid.length);
        // Batch count follows the format magic and committed frontier.
        Arrays.fill(corrupted, 12, 16, (byte) 0x7f);
        assertThrows(IOException.class, () -> serializer().deserialize(1, corrupted));
        Arrays.fill(corrupted, 12, 16, (byte) 0xff);
        assertThrows(IOException.class, () -> serializer().deserialize(1, corrupted));
    }

    private static CommittableSummary<String> summary(
            int writer, int parallelism, Long checkpoint, int count) {
        return new CommittableSummary<>(writer, parallelism, checkpoint, count, 0, 0);
    }

    private static FlinkGlobalCommitStateSerializer<String> serializer() {
        return new FlinkGlobalCommitStateSerializer<>(STRINGS);
    }
}

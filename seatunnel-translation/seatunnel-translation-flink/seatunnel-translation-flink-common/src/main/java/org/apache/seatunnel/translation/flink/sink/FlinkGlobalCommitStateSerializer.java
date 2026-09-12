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

import org.apache.seatunnel.translation.flink.sink.FlinkGlobalCommitState.Batch;
import org.apache.seatunnel.translation.flink.sink.FlinkGlobalCommitState.WriterState;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * SeaTunnel global state v3, with a read-only migration of Flink 1.15/1.18 global state v2.
 *
 * <p>The legacy layout is defined by Flink GlobalCommitterSerializer and
 * CommittableCollectorSerializer (release-1.18.1). Reading the nested bytes preserves producer
 * segments which Flink's deserializer otherwise merges into the current subtask. Legacy state does
 * not store producer identities, parallelism, or the original expected message count. Migration is
 * restricted to the routed Paimon adapter, which emits exactly one CommitWrapper per producer per
 * checkpoint, including empty Paimon commits. The first upgrade must keep writer parallelism. A
 * positive declaration must be validated against the restored checkpoint by the operator; zero
 * means no evidence and rejects every nonempty legacy state. The current writer count is not proof
 * of the original producer set. Version 3 records that set explicitly and needs no declaration.
 */
final class FlinkGlobalCommitStateSerializer<CommT, GlobalCommT>
        implements SimpleVersionedSerializer<FlinkGlobalCommitState<CommT, GlobalCommT>> {

    private static final int GLOBAL_MAGIC = -1189141205;
    private static final int COLLECTOR_MAGIC = -1189141204;
    private static final int STATE_MAGIC = 0x53544743;

    private final SimpleVersionedSerializer<CommT> committableSerializer;
    private final SimpleVersionedSerializer<GlobalCommT> globalSerializer;
    private final int declaredLegacyWriters;

    FlinkGlobalCommitStateSerializer(
            SimpleVersionedSerializer<CommT> committableSerializer,
            SimpleVersionedSerializer<GlobalCommT> globalSerializer) {
        this(committableSerializer, globalSerializer, 0);
    }

    public FlinkGlobalCommitStateSerializer(
            SimpleVersionedSerializer<CommT> committableSerializer,
            SimpleVersionedSerializer<GlobalCommT> globalSerializer,
            int declaredLegacyWriters) {
        this.committableSerializer = Objects.requireNonNull(committableSerializer);
        this.globalSerializer = Objects.requireNonNull(globalSerializer);
        if (declaredLegacyWriters < 0) {
            throw new IllegalArgumentException(
                    "Declared legacy writer parallelism cannot be negative");
        }
        this.declaredLegacyWriters = declaredLegacyWriters;
    }

    @Override
    public int getVersion() {
        return 3;
    }

    @Override
    public byte[] serialize(FlinkGlobalCommitState<CommT, GlobalCommT> state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(STATE_MAGIC);
        out.writeLong(state.getLastCommittedCheckpointId());
        writeList(out, globalSerializer, state.getLegacyGlobalCommittables());
        out.writeInt(state.getBatches().size());
        for (Batch<CommT> batch : state.getBatches().values()) {
            out.writeLong(batch.getCheckpointId());
            out.writeBoolean(batch.legacy);
            out.writeInt(batch.expectedWriters);
            out.writeInt(batch.writers.size());
            for (Map.Entry<Integer, WriterState<CommT>> entry : batch.writers.entrySet()) {
                out.writeInt(entry.getKey());
                WriterState<CommT> writer = entry.getValue();
                out.writeInt(writer.expectedCommittables);
                out.writeInt(writer.pendingCommittables);
                writeList(out, committableSerializer, writer.committables);
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public FlinkGlobalCommitState<CommT, GlobalCommT> deserialize(int version, byte[] bytes)
            throws IOException {
        DataInputStream in = input(bytes);
        FlinkGlobalCommitState<CommT, GlobalCommT> state;
        if (version == 2) {
            state = readLegacy(in);
        } else if (version == 3) {
            state = readCurrent(in);
        } else {
            throw new IOException("Unsupported global commit state version: " + version);
        }
        requireEnd(in);
        return state;
    }

    private FlinkGlobalCommitState<CommT, GlobalCommT> readCurrent(DataInputStream in)
            throws IOException {
        requireMagic(in, STATE_MAGIC);
        FlinkGlobalCommitState<CommT, GlobalCommT> state = new FlinkGlobalCommitState<>();
        state.restoreFrontier(in.readLong());
        state.addLegacyGlobalCommittables(readList(in, globalSerializer::deserialize));
        int batchCount = readCount(in, 17);
        for (int i = 0; i < batchCount; i++) {
            Batch<CommT> batch = new Batch<>(readCheckpoint(in));
            batch.legacy = in.readBoolean();
            batch.expectedWriters = in.readInt();
            if (batch.expectedWriters != -1 && batch.expectedWriters < 1) {
                throw new IOException("Invalid producer parallelism in global state");
            }
            int writerCount = readCount(in, 20);
            for (int j = 0; j < writerCount; j++) {
                int writerId = in.readInt();
                if (writerId < 0
                        || (batch.expectedWriters > 0 && writerId >= batch.expectedWriters)) {
                    throw new IOException("Invalid producer identity in global state");
                }
                WriterState<CommT> writer = new WriterState<>();
                int expected = in.readInt();
                int pending = in.readInt();
                writer.committables.addAll(readList(in, committableSerializer::deserialize));
                if (expected == -1 && pending == 0) {
                    // A lineage message can precede its summary; it remains incomplete.
                } else {
                    writer.setSummary(expected, pending, 0);
                }
                if (batch.writers.putIfAbsent(writerId, writer) != null) {
                    throw new IOException("Duplicate producer identity in global state");
                }
            }
            if (batch.legacy && !batch.isComplete()) {
                throw new IOException("Incomplete migrated global batch");
            }
            state.restoreBatch(batch);
        }
        return state;
    }

    private FlinkGlobalCommitState<CommT, GlobalCommT> readLegacy(DataInputStream in)
            throws IOException {
        requireMagic(in, GLOBAL_MAGIC);
        FlinkGlobalCommitState<CommT, GlobalCommT> state = new FlinkGlobalCommitState<>();
        if (in.readBoolean()) {
            state.addLegacyGlobalCommittables(readList(in, globalSerializer::deserialize));
        }
        if (!state.getLegacyGlobalCommittables().isEmpty() && declaredLegacyWriters == 0) {
            throw missingLegacyProof();
        }
        requireVersion(in.readInt(), 2, "legacy collector");
        DataInputStream collector = input(readBytes(in));
        requireMagic(collector, COLLECTOR_MAGIC);
        List<Batch<CommT>> batches = readList(collector, this::readLegacyBatch);
        requireEnd(collector);
        batches.sort(Comparator.comparingLong(Batch::getCheckpointId));
        for (Batch<CommT> batch : batches) {
            if (batch.legacyCommitted) {
                if (!state.getBatches().isEmpty()
                        || batch.getCheckpointId() <= state.getLastCommittedCheckpointId()) {
                    throw new IOException("Inconsistent committed frontier in legacy global state");
                }
                state.restoreFrontier(batch.getCheckpointId());
            } else {
                state.restoreBatch(batch);
            }
        }
        return state;
    }

    private Batch<CommT> readLegacyBatch(int version, byte[] bytes) throws IOException {
        requireVersion(version, 0, "legacy checkpoint");
        if (declaredLegacyWriters == 0) {
            throw missingLegacyProof();
        }
        DataInputStream in = input(bytes);
        Batch<CommT> batch = new Batch<>(readCheckpoint(in));
        batch.legacy = true;
        batch.expectedWriters = declaredLegacyWriters;
        List<WriterState<CommT>> writers = readList(in, this::readLegacyWriter);
        requireEnd(in);
        if (writers.size() == declaredLegacyWriters
                && writers.stream().allMatch(writer -> writer.legacyDrained)) {
            // Flink retains fully drained collectors until a later traversal removes them.
            // Every original producer is proven committed; this is not a partially committed batch.
            batch.legacyCommitted = true;
            return batch;
        }
        if (writers.stream().anyMatch(writer -> writer.legacyDrained)) {
            throw new IOException("Cannot migrate a partially drained legacy global batch");
        }
        for (int index = 0; index < writers.size(); index++) {
            batch.writers.put(index, writers.get(index));
        }
        if (!batch.isComplete()) {
            throw new IOException(
                    "Incomplete legacy global batch at checkpoint "
                            + batch.getCheckpointId()
                            + "; legacy migration requires unchanged writer parallelism and all producer segments");
        }
        return batch;
    }

    private WriterState<CommT> readLegacyWriter(int version, byte[] bytes) throws IOException {
        requireVersion(version, 0, "legacy producer");
        DataInputStream in = input(bytes);
        WriterState<CommT> writer = new WriterState<>();
        writer.committables.addAll(readList(in, this::readLegacyRequest));
        int expected = in.readInt();
        int drained = in.readInt();
        int failed = in.readInt();
        requireEnd(in);
        if (expected == 1 && drained == 1 && failed == 0 && writer.committables.isEmpty()) {
            writer.legacyDrained = true;
            return writer;
        }
        if (drained != 0 || expected != 1 || writer.committables.size() != 1) {
            throw new IOException(
                    "Legacy routed Paimon recovery requires exactly one undrained commit wrapper per producer");
        }
        writer.setSummary(expected, 0, failed);
        return writer;
    }

    private CommT readLegacyRequest(int version, byte[] bytes) throws IOException {
        requireVersion(version, 0, "legacy commit request");
        DataInputStream in = input(bytes);
        int payloadVersion = in.readInt();
        CommT committable = committableSerializer.deserialize(payloadVersion, readBytes(in));
        int retries = in.readInt();
        int status = in.readInt();
        // Flink request states: RECEIVED=0, RETRY=1, FAILED=2, COMMITTED=3.
        if (retries < 0 || status < 0 || status > 3 || status == 2) {
            throw new IOException("Invalid or failed legacy global commit request");
        }
        requireEnd(in);
        return committable;
    }

    private static long readCheckpoint(DataInputStream in) throws IOException {
        long checkpoint = in.readLong();
        if (checkpoint < 0) {
            throw new IOException("Negative global commit checkpoint identifier");
        }
        return checkpoint;
    }

    private static IOException missingLegacyProof() {
        return new IOException(
                "Nonempty legacy global state requires a declared original writer parallelism bound to the restored checkpoint");
    }

    private static <T> List<T> readList(DataInputStream in, Decoder<T> decoder) throws IOException {
        int version = in.readInt();
        int size = readCount(in, Integer.BYTES);
        List<T> result = new ArrayList<>();
        for (int index = 0; index < size; index++) {
            result.add(decoder.deserialize(version, readBytes(in)));
        }
        return result;
    }

    private static <T> void writeList(
            DataOutputStream out, SimpleVersionedSerializer<T> serializer, List<T> values)
            throws IOException {
        out.writeInt(serializer.getVersion());
        out.writeInt(values.size());
        for (T value : values) {
            byte[] bytes = serializer.serialize(value);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    private static int readCount(DataInputStream in, int minimumBytes) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > in.available() / minimumBytes) {
            throw new IOException("Invalid element count in global commit state");
        }
        return count;
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > in.available()) {
            throw new IOException("Invalid byte length in global commit state");
        }
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        return bytes;
    }

    private static DataInputStream input(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    private static void requireMagic(DataInputStream in, int expected) throws IOException {
        if (in.readInt() != expected) {
            throw new IOException("Unexpected magic number in global commit state");
        }
    }

    private static void requireVersion(int actual, int expected, String component)
            throws IOException {
        if (actual != expected) {
            throw new IOException("Unsupported " + component + " version: " + actual);
        }
    }

    private static void requireEnd(DataInputStream in) throws IOException {
        if (in.available() != 0) {
            throw new IOException("Trailing bytes in global commit state");
        }
    }

    private interface Decoder<T> {
        T deserialize(int version, byte[] bytes) throws IOException;
    }
}

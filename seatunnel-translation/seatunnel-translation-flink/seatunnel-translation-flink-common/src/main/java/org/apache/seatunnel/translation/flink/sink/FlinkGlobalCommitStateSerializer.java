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
import java.util.Map;
import java.util.Objects;

/**
 * Persists complete producer identities and pending contributions for routed global commits.
 *
 * <p>This format cannot read the original Flink global committer state: that state does not retain
 * enough producer information to prove a batch complete after redistribution. Unknown formats
 * require a fresh snapshot; the current writer parallelism is never substituted for historical
 * producer identities.
 */
final class FlinkGlobalCommitStateSerializer<CommT>
        implements SimpleVersionedSerializer<FlinkGlobalCommitState<CommT>> {

    private static final int STATE_MAGIC = 0x53544743;

    private final SimpleVersionedSerializer<CommT> committableSerializer;

    FlinkGlobalCommitStateSerializer(SimpleVersionedSerializer<CommT> committableSerializer) {
        this.committableSerializer = Objects.requireNonNull(committableSerializer);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(FlinkGlobalCommitState<CommT> state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(STATE_MAGIC);
        out.writeLong(state.getLastCommittedCheckpointId());
        out.writeInt(state.getBatches().size());
        for (Batch<CommT> batch : state.getBatches().values()) {
            out.writeLong(batch.getCheckpointId());
            out.writeInt(batch.expectedWriters);
            out.writeInt(batch.writers.size());
            for (Map.Entry<Integer, WriterState<CommT>> entry : batch.writers.entrySet()) {
                out.writeInt(entry.getKey());
                WriterState<CommT> writer = entry.getValue();
                out.writeInt(writer.expectedCommittables);
                out.writeInt(writer.pendingCommittables);
                out.writeInt(committableSerializer.getVersion());
                out.writeInt(writer.committables.size());
                for (CommT committable : writer.committables) {
                    byte[] payload = committableSerializer.serialize(committable);
                    out.writeInt(payload.length);
                    out.write(payload);
                }
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public FlinkGlobalCommitState<CommT> deserialize(int version, byte[] bytes) throws IOException {
        if (version != getVersion()) {
            throw new IOException(
                    "Unsupported routed global commit state version: "
                            + version
                            + "; start from a fresh snapshot");
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        if (in.readInt() != STATE_MAGIC) {
            throw new IOException(
                    "Unsupported routed global commit state format; start from a fresh snapshot");
        }
        FlinkGlobalCommitState<CommT> state = new FlinkGlobalCommitState<>();
        state.restoreFrontier(in.readLong());
        int batchCount = readCount(in, 16);
        for (int i = 0; i < batchCount; i++) {
            long checkpointId = in.readLong();
            if (checkpointId < 0) {
                throw new IOException("Negative global commit checkpoint identifier");
            }
            Batch<CommT> batch = new Batch<>(checkpointId);
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
                int payloadVersion = in.readInt();
                int payloadCount = readCount(in, Integer.BYTES);
                for (int payload = 0; payload < payloadCount; payload++) {
                    writer.committables.add(
                            committableSerializer.deserialize(payloadVersion, readBytes(in)));
                }
                if (expected == -1 && pending == 0) {
                    // A lineage message can precede its summary; it remains incomplete.
                } else {
                    writer.setSummary(expected, pending, 0);
                }
                if (batch.writers.putIfAbsent(writerId, writer) != null) {
                    throw new IOException("Duplicate producer identity in global state");
                }
            }
            state.restoreBatch(batch);
        }
        if (in.available() != 0) {
            throw new IOException("Trailing bytes in global commit state");
        }
        return state;
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
}

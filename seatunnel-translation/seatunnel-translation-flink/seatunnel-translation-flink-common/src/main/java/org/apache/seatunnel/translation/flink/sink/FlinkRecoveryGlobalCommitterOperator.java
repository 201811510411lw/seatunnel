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
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Completes recovered routed-sink commits without requiring another checkpoint from the writer.
 *
 * <p>The local Flink committer can replay messages after global state initialization. Both message
 * arrival and checkpoint completion therefore advance the same ordered queue. A batch needs the
 * checkpoint's completion proof AND every upstream committer's complete contribution. In
 * particular, committing the first arriving writer alone can make Paimon's idempotence discard the
 * other writers. This operator only belongs downstream of the local committer in {@link
 * FlinkRecoverySink}.
 */
final class FlinkRecoveryGlobalCommitterOperator<CommT, GlobalCommT>
        extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<CommittableMessage<CommT>, Void>, BoundedOneInput {

    private final Sink<?, CommT, ?, GlobalCommT> sink;
    private final int writerParallelism;
    private final int declaredLegacyWriterParallelism;
    private final long declaredLegacyCheckpointId;

    private transient GlobalCommitter<CommT, GlobalCommT> committer;
    private transient ListState<FlinkGlobalCommitState<CommT, GlobalCommT>> checkpointState;
    private transient FlinkGlobalCommitState<CommT, GlobalCommT> pending;
    private long lastCompletedCheckpointId = -1;
    private long restoredCheckpointId = -1;

    FlinkRecoveryGlobalCommitterOperator(
            Sink<?, CommT, ?, GlobalCommT> sink, int writerParallelism) {
        this(sink, writerParallelism, 0, -1);
    }

    FlinkRecoveryGlobalCommitterOperator(
            Sink<?, CommT, ?, GlobalCommT> sink,
            int writerParallelism,
            int declaredLegacyWriterParallelism,
            long declaredLegacyCheckpointId) {
        this.sink = sink;
        this.writerParallelism = writerParallelism;
        this.declaredLegacyWriterParallelism = declaredLegacyWriterParallelism;
        this.declaredLegacyCheckpointId = declaredLegacyCheckpointId;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        committer =
                sink.createGlobalCommitter()
                        .orElseThrow(
                                () -> new IOException("Routed sink requires a global committer"));
        checkpointState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(
                                        new ListStateDescriptor<>(
                                                "streaming_committer_raw_states",
                                                BytePrimitiveArraySerializer.INSTANCE)),
                        new FlinkGlobalCommitStateSerializer<>(
                                sink.getCommittableSerializer().get(),
                                sink.getGlobalCommittableSerializer().get(),
                                context.isRestored()
                                                && declaredLegacyWriterParallelism
                                                        == writerParallelism
                                                && declaredLegacyCheckpointId
                                                        == context.getRestoredCheckpointId()
                                                                .getAsLong()
                                        ? declaredLegacyWriterParallelism
                                        : 0));
        pending = new FlinkGlobalCommitState<>();
        if (context.isRestored()) {
            // The global operator always has parallelism/maxParallelism 1. Never discard a second
            // state partition: that would make an unexpected topology change silently lose commits.
            boolean restored = false;
            for (FlinkGlobalCommitState<CommT, GlobalCommT> state : checkpointState.get()) {
                if (restored) {
                    throw new IOException("Expected one routed global committer state partition");
                }
                pending = state;
                restored = true;
            }
            if (pending.getLastCommittedCheckpointId() == Long.MAX_VALUE
                    || pending.getBatches().containsKey(Long.MAX_VALUE)) {
                throw new IOException(
                        "Cannot restore routed global commit state after end of input; "
                                + "use a non-draining savepoint or a checkpoint before termination");
            }
            lastCompletedCheckpointId = context.getRestoredCheckpointId().getAsLong();
            restoredCheckpointId = lastCompletedCheckpointId;
            commitRecoveredAggregates();
            commitReadyBatches();
        }
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<CommT>> element) throws Exception {
        CommittableMessage<CommT> message = element.getValue();
        long checkpointId = message.getCheckpointId().orElse(Long.MAX_VALUE);
        if (checkpointId <= pending.getLastCommittedCheckpointId()) {
            return;
        }
        pending.addMessage(message);
        commitReadyBatches();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        lastCompletedCheckpointId = Math.max(lastCompletedCheckpointId, checkpointId);
        commitReadyBatches();
    }

    private void commitRecoveredAggregates() throws IOException, InterruptedException {
        List<GlobalCommT> recovered = pending.getLegacyGlobalCommittables();
        if (!recovered.isEmpty()) {
            requireCommitted(committer.filterRecoveredCommittables(recovered));
            pending.clearLegacyGlobalCommittables();
        }
    }

    private void commitReadyBatches() throws IOException, InterruptedException {
        while (!pending.getBatches().isEmpty()) {
            FlinkGlobalCommitState.Batch<CommT> batch =
                    pending.getBatches().firstEntry().getValue();
            if (batch.getCheckpointId() > lastCompletedCheckpointId || !batch.isComplete()) {
                // A newer Paimon commit identifier must never overtake an incomplete older batch.
                return;
            }
            List<CommT> committables = batch.getCommittables();
            if (!committables.isEmpty()) {
                List<GlobalCommT> combined =
                        Collections.singletonList(committer.combine(committables));
                requireCommitted(
                        batch.getCheckpointId() <= restoredCheckpointId
                                ? committer.filterRecoveredCommittables(combined)
                                : committer.commit(combined));
            }
            pending.markCommitted(batch.getCheckpointId());
        }
    }

    private void requireCommitted(List<GlobalCommT> remaining) throws IOException {
        if (remaining != null && !remaining.isEmpty()) {
            throw new IOException(
                    "Incomplete routed global commit; retaining checkpoint state for recovery");
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        checkpointState.update(Collections.singletonList(pending));
    }

    @Override
    public void endInput() throws Exception {
        lastCompletedCheckpointId = Long.MAX_VALUE;
        commitReadyBatches();
        if (!pending.getBatches().isEmpty()) {
            throw new IOException("Routed sink ended with an incomplete global commit batch");
        }
        committer.endOfInput();
    }

    @Override
    public void close() throws Exception {
        try {
            if (committer != null) {
                committer.close();
            }
        } finally {
            super.close();
        }
    }
}

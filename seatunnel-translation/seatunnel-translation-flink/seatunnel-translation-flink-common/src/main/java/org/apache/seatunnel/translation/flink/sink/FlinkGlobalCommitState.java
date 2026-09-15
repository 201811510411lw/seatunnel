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

import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/** Global commit batches retain the original producer set, including idle producers. */
final class FlinkGlobalCommitState<CommT> {

    private final NavigableMap<Long, Batch<CommT>> batches = new TreeMap<>();
    private long lastCommittedCheckpointId = -1L;

    public NavigableMap<Long, Batch<CommT>> getBatches() {
        return Collections.unmodifiableNavigableMap(batches);
    }

    public long getLastCommittedCheckpointId() {
        return lastCommittedCheckpointId;
    }

    /**
     * Advances the frontier only after the oldest batch is complete and its external commit
     * succeeds.
     */
    public void markCommitted(long checkpointId) {
        if (checkpointId < lastCommittedCheckpointId
                || batches.isEmpty()
                || batches.firstKey() != checkpointId
                || !batches.firstEntry().getValue().isComplete()) {
            throw new IllegalArgumentException(
                    "Only the oldest complete global batch can be committed");
        }
        lastCommittedCheckpointId = checkpointId;
        batches.remove(checkpointId);
    }

    /** Retains new producer contributions and ignores replay behind the committed frontier. */
    public void addMessage(CommittableMessage<CommT> message) throws IOException {
        long checkpointId = message.getCheckpointId().orElse(Long.MAX_VALUE);
        if (checkpointId < 0) {
            throw new IOException("Negative global commit checkpoint identifier");
        }
        if (checkpointId <= lastCommittedCheckpointId) {
            return;
        }
        Batch<CommT> batch = batches.computeIfAbsent(checkpointId, Batch::new);
        batch.addMessage(message);
    }

    /** Restores the persisted frontier, allowing -1 only for a job with no completed commit. */
    void restoreFrontier(long checkpointId) throws IOException {
        if (checkpointId < -1L) {
            throw new IOException("Invalid restored global commit frontier");
        }
        lastCommittedCheckpointId = checkpointId;
    }

    /** Restores an uncommitted batch while rejecting duplicate or already committed boundaries. */
    void restoreBatch(Batch<CommT> batch) throws IOException {
        if (batch.checkpointId <= lastCommittedCheckpointId
                || batches.putIfAbsent(batch.checkpointId, batch) != null) {
            throw new IOException("Duplicate or already committed global batch in state");
        }
    }

    public static final class Batch<CommT> {

        private final long checkpointId;
        final NavigableMap<Integer, WriterState<CommT>> writers = new TreeMap<>();
        int expectedWriters = -1;

        Batch(long checkpointId) {
            this.checkpointId = checkpointId;
        }

        public long getCheckpointId() {
            return checkpointId;
        }

        /**
         * Requires every declared producer identity, summary, and committable before committing.
         */
        public boolean isComplete() {
            if (expectedWriters < 1 || writers.size() != expectedWriters) {
                return false;
            }
            for (int index = 0; index < expectedWriters; index++) {
                WriterState<CommT> writer = writers.get(index);
                if (writer == null
                        || !writer.hasSummary()
                        || writer.committables.size() != writer.expectedCommittables) {
                    return false;
                }
            }
            return true;
        }

        public List<CommT> getCommittables() {
            List<CommT> result = new ArrayList<>();
            writers.values().forEach(writer -> result.addAll(writer.committables));
            return result;
        }

        /** Validates producer identities and matching summaries before retaining a contribution. */
        private void addMessage(CommittableMessage<CommT> message) throws IOException {
            int subtask = message.getSubtaskId();
            if (subtask < 0 || (expectedWriters > 0 && subtask >= expectedWriters)) {
                throw new IOException("Invalid producer identity in global commit batch");
            }
            WriterState<CommT> writer =
                    writers.computeIfAbsent(subtask, ignored -> new WriterState<>());
            if (message instanceof CommittableSummary) {
                CommittableSummary<CommT> summary = (CommittableSummary<CommT>) message;
                int producerCount = summary.getNumberOfSubtasks();
                if (producerCount < 1
                        || subtask >= producerCount
                        || (expectedWriters != -1 && expectedWriters != producerCount)
                        || (!writers.isEmpty() && writers.lastKey() >= producerCount)) {
                    throw new IOException("Inconsistent producer set in global commit summary");
                }
                writer.setSummary(
                        summary.getNumberOfCommittables(),
                        summary.getNumberOfPendingCommittables(),
                        summary.getNumberOfFailedCommittables());
                expectedWriters = producerCount;
            } else if (message instanceof CommittableWithLineage) {
                if (writer.hasSummary()
                        && writer.committables.size() >= writer.expectedCommittables) {
                    throw new IOException("More global committables than declared by producer");
                }
                writer.committables.add(((CommittableWithLineage<CommT>) message).getCommittable());
            } else {
                throw new IOException("Unsupported global committable message type");
            }
        }
    }

    static final class WriterState<CommT> {

        int expectedCommittables = -1;
        int pendingCommittables;
        final List<CommT> committables = new ArrayList<>();

        boolean hasSummary() {
            return expectedCommittables >= 0;
        }

        /** Accepts an unchanged summary replay but rejects failures or conflicting counts. */
        void setSummary(int expected, int pending, int failed) throws IOException {
            if (expected < 0 || pending < 0 || pending > expected || failed != 0) {
                throw new IOException("Invalid or failed global committable summary");
            }
            if (hasSummary()
                    && (expectedCommittables != expected || pendingCommittables != pending)) {
                throw new IOException("Conflicting duplicate global committable summary");
            }
            if (committables.size() > expected) {
                throw new IOException("Received more global committables than producer declared");
            }
            expectedCommittables = expected;
            pendingCommittables = pending;
        }
    }
}

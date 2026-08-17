/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlinkSinkWriterCheckpointBoundaryTest {

    @Test
    void snapshotsBeforeRecordsWrittenAfterPrepareCommit() throws Exception {
        BoundaryWriter delegate = new BoundaryWriter();
        FlinkSinkWriter<SeaTunnelRow, String, String> writer =
                new FlinkSinkWriter<>(delegate, 1L, new BoundaryContext());

        writer.prepareCommit(false);
        writer.write(new SeaTunnelRow(0), null);
        List<FlinkWriterState<String>> states = writer.snapshotState();

        assertEquals(Collections.singletonList(1L), delegate.snapshotCalls);
        assertEquals("state-1", states.get(0).getState());
    }

    private static final class BoundaryWriter
            implements SinkWriter<SeaTunnelRow, String, String> {
        private final java.util.ArrayList<Long> snapshotCalls = new java.util.ArrayList<>();
        private boolean prepared;
        private boolean wroteAfterPrepare;

        @Override
        public void write(SeaTunnelRow row) {
            if (prepared) {
                wroteAfterPrepare = true;
            }
        }

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public Optional<String> prepareCommit(long checkpointId) {
            prepared = true;
            return Optional.of("commit-" + checkpointId);
        }

        @Override
        public List<String> snapshotState(long checkpointId) {
            if (wroteAfterPrepare) {
                throw new AssertionError("snapshot was not adjacent to prepareCommit");
            }
            snapshotCalls.add(checkpointId);
            prepared = false;
            return Collections.singletonList("state-" + checkpointId);
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() throws IOException {}
    }

    private static final class BoundaryContext implements SinkWriter.Context {
        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public MetricsContext getMetricsContext() {
            return new MetricsContext() {
                @Override
                public Counter counter(String name) {
                    return new NoopCounter(name);
                }

                @Override
                public <C extends Counter> C counter(String name, C counter) {
                    return counter;
                }

                @Override
                public Meter meter(String name) {
                    return new NoopMeter(name);
                }

                @Override
                public <M extends Meter> M meter(String name, M meter) {
                    return meter;
                }
            };
        }

        @Override
        public EventListener getEventListener() {
            return event -> {};
        }
    }

    private static final class NoopCounter implements Counter {
        private final String name;

        private NoopCounter(String name) {
            this.name = name;
        }

        public void inc() {}

        public void inc(long n) {}

        public void dec() {}

        public void dec(long n) {}

        public void set(long n) {}

        public long getCount() {
            return 0;
        }

        public String name() {
            return name;
        }

        public org.apache.seatunnel.api.common.metrics.Unit unit() {
            return org.apache.seatunnel.api.common.metrics.Unit.COUNT;
        }
    }

    private static final class NoopMeter implements Meter {
        private final String name;

        private NoopMeter(String name) {
            this.name = name;
        }

        public void markEvent() {}

        public void markEvent(long n) {}

        public double getRate() {
            return 0;
        }

        public long getCount() {
            return 0;
        }

        public String name() {
            return name;
        }

        public org.apache.seatunnel.api.common.metrics.Unit unit() {
            return org.apache.seatunnel.api.common.metrics.Unit.COUNT;
        }
    }
}

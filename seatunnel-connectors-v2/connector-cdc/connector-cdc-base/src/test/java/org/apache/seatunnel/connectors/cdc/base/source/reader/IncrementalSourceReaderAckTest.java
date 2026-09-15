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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import org.apache.seatunnel.api.common.metrics.Gauge;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.event.CdcProgressEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CdcProgressPhase;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsAckEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CompletedSnapshotSplitsReportEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IncrementalSourceReaderAckTest {

    @Test
    void shouldRetainReportedSplitsInCheckpointUntilAcknowledged() {
        try (Fixture fixture = new Fixture()) {
            fixture.reader.addSplits(Collections.singletonList(finishedSplit("split-0")));

            assertEquals(Collections.singletonList("split-0"), fixture.lastReportIds());
            assertEquals(
                    Collections.singletonList("split-0"),
                    splitIds(fixture.reader.snapshotState(1L)));
            fixture.ack("split-0");
            assertEquals(Collections.emptyList(), fixture.reader.snapshotState(2L));
        }
    }

    @Test
    void shouldOnlyRemoveAcknowledgedSplitsAndTolerateDuplicateAcknowledgements() {
        try (Fixture fixture = new Fixture()) {
            fixture.reader.addSplits(
                    Arrays.asList(finishedSplit("split-0"), finishedSplit("split-1")));
            fixture.ack("split-0");
            fixture.ack("split-0", "unknown");
            assertEquals(
                    Collections.singletonList("split-1"),
                    splitIds(fixture.reader.snapshotState(1L)));

            fixture.reader.addSplits(Collections.singletonList(finishedSplit("split-2")));
            assertEquals(Arrays.asList("split-1", "split-2"), fixture.lastReportIds());
            fixture.ack("split-2");
            assertEquals(
                    Collections.singletonList("split-1"),
                    splitIds(fixture.reader.snapshotState(2L)));
            fixture.ack("split-1");
            assertEquals(Collections.emptyList(), fixture.reader.snapshotState(3L));
        }
    }

    @Test
    void shouldReportCheckpointedSplitsWithTheirWatermarksAfterRestore() {
        try (Fixture original = new Fixture();
                Fixture restored = new Fixture()) {
            original.reader.addSplits(Collections.singletonList(finishedSplit("split-0")));
            List<SourceSplitBase> checkpoint = original.reader.snapshotState(1L);
            original.ack("split-0");

            restored.reader.addSplits(checkpoint);
            assertEquals(Collections.singletonList("split-0"), restored.lastReportIds());
            SnapshotSplitWatermark watermark =
                    restored.reports.get(0).getCompletedSnapshotSplitWatermarks().get(0);
            assertEquals(new TestOffset(10), watermark.getLowWatermark());
            assertEquals(new TestOffset(20), watermark.getHighWatermark());
            assertEquals(
                    Collections.singletonList("split-0"),
                    splitIds(restored.reader.snapshotState(2L)));
            restored.ack("split-0");
            assertEquals(Collections.emptyList(), restored.reader.snapshotState(3L));
        }
    }

    @Test
    void shouldPreserveProgressUpdatesWhileHandlingAcknowledgements() {
        try (Fixture fixture = new Fixture()) {
            @SuppressWarnings("rawtypes")
            ArgumentCaptor<Gauge> gauge = ArgumentCaptor.forClass(Gauge.class);
            Mockito.verify(fixture.metrics)
                    .gauge(Mockito.eq("chronoforgeCdcProgress"), gauge.capture());
            fixture.reader.addSplits(Collections.singletonList(finishedSplit("split-0")));
            fixture.reader.handleSourceEvent(new CdcProgressEvent(CdcProgressPhase.INCREMENTAL));
            assertEquals(
                    Collections.singletonList("split-0"),
                    splitIds(fixture.reader.snapshotState(1L)));

            fixture.ack("split-0");
            assertEquals("{\"version\":1,\"phase\":\"INCREMENTAL\"}", gauge.getValue().getValue());
            assertEquals(Collections.emptyList(), fixture.reader.snapshotState(2L));
        }
    }

    private static SnapshotSplit finishedSplit(String id) {
        return new SnapshotSplit(
                id,
                new TableId("test", null, "orders"),
                null,
                new Object[] {1},
                new Object[] {2},
                new TestOffset(10),
                new TestOffset(20));
    }

    private static List<String> splitIds(List<SourceSplitBase> splits) {
        return splits.stream().map(SourceSplitBase::splitId).sorted().collect(Collectors.toList());
    }

    private static class TestOffset extends Offset {
        private TestOffset(long position) {
            offset = Collections.singletonMap("position", Long.toString(position));
        }

        @Override
        public int compareTo(Offset other) {
            return Long.compare(
                    Long.parseLong(offset.get("position")),
                    Long.parseLong(other.getOffset().get("position")));
        }
    }

    private static class Fixture implements AutoCloseable {
        private final List<CompletedSnapshotSplitsReportEvent> reports = new ArrayList<>();
        private final MetricsContext metrics = Mockito.mock(MetricsContext.class);
        private final IncrementalSourceReader<Object, SourceConfig> reader;

        @SuppressWarnings("unchecked")
        private Fixture() {
            SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
            Mockito.when(context.getMetricsContext()).thenReturn(metrics);
            Mockito.doAnswer(
                            invocation -> {
                                SourceEvent event = invocation.getArgument(0);
                                if (event instanceof CompletedSnapshotSplitsReportEvent) {
                                    reports.add((CompletedSnapshotSplitsReportEvent) event);
                                }
                                return null;
                            })
                    .when(context)
                    .sendSourceEventToEnumerator(Mockito.any());
            reader =
                    new IncrementalSourceReader<>(
                            Mockito.mock(DataSourceDialect.class),
                            new LinkedBlockingQueue<>(),
                            () -> Mockito.mock(IncrementalSourceSplitReader.class),
                            Mockito.mock(RecordEmitter.class),
                            Mockito.mock(SourceReaderOptions.class),
                            context,
                            Mockito.mock(SourceConfig.class),
                            Mockito.mock(DebeziumDeserializationSchema.class));
        }

        private List<String> lastReportIds() {
            return reports.get(reports.size() - 1).getCompletedSnapshotSplitWatermarks().stream()
                    .map(SnapshotSplitWatermark::getSplitId)
                    .sorted()
                    .collect(Collectors.toList());
        }

        private void ack(String... splitIds) {
            reader.handleSourceEvent(new CompletedSnapshotSplitsAckEvent(Arrays.asList(splitIds)));
        }

        @Override
        public void close() {
            reader.close();
        }
    }
}

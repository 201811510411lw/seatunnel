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
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.event.CdcProgressEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CdcProgressPhase;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SourceSplitStateBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IncrementalSourceReaderCdcProgressTest {

    @Test
    void readerZeroRegistersFixedGaugeAndUpdatesItFromEnumeratorEvents() {
        MetricsContext metrics = Mockito.mock(MetricsContext.class);
        IncrementalSourceReader<Object, SourceConfig> reader = reader(0, metrics);
        @SuppressWarnings("rawtypes")
        ArgumentCaptor<Gauge> gauge = ArgumentCaptor.forClass(Gauge.class);
        Mockito.verify(metrics).gauge(Mockito.eq("chronoforgeCdcProgress"), gauge.capture());
        assertEquals("{\"version\":1,\"phase\":\"SNAPSHOT\"}", gauge.getValue().getValue());

        reader.handleSourceEvent(new CdcProgressEvent(CdcProgressPhase.INCREMENTAL));

        assertEquals("{\"version\":1,\"phase\":\"INCREMENTAL\"}", gauge.getValue().getValue());
    }

    @Test
    void otherReadersDoNotRegisterTheGauge() {
        MetricsContext metrics = Mockito.mock(MetricsContext.class);

        reader(1, metrics);

        Mockito.verify(metrics, Mockito.never())
                .gauge(Mockito.anyString(), Mockito.any(Gauge.class));
    }

    @SuppressWarnings("unchecked")
    private IncrementalSourceReader<Object, SourceConfig> reader(
            int subtaskId, MetricsContext metrics) {
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getIndexOfSubtask()).thenReturn(subtaskId);
        Mockito.when(context.getMetricsContext()).thenReturn(metrics);
        BlockingQueue<RecordsWithSplitIds<SourceRecords>> queue = new LinkedBlockingQueue<>();
        IncrementalSourceSplitReader<SourceConfig> splitReader =
                Mockito.mock(IncrementalSourceSplitReader.class);
        return new IncrementalSourceReader<>(
                Mockito.mock(DataSourceDialect.class),
                queue,
                () -> splitReader,
                Mockito.mock(RecordEmitter.class),
                Mockito.mock(SourceReaderOptions.class),
                context,
                Mockito.mock(SourceConfig.class),
                Mockito.mock(DebeziumDeserializationSchema.class));
    }
}

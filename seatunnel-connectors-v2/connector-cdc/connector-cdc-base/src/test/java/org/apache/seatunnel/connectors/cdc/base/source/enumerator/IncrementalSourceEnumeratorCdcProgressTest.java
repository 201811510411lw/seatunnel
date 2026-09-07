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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.cdc.base.source.event.CdcProgressEvent;
import org.apache.seatunnel.connectors.cdc.base.source.event.CdcProgressPhase;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IncrementalSourceEnumeratorCdcProgressTest {

    @Test
    void publishesCurrentPhaseOnlyToReaderZeroAndResendsAfterRegistration() {
        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<SourceSplitBase> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        SplitAssigner splitAssigner = Mockito.mock(SplitAssigner.class);
        Mockito.when(context.registeredReaders()).thenReturn(new HashSet<>(Arrays.asList(0, 1)));
        Mockito.when(splitAssigner.getCdcProgressPhase())
                .thenReturn(CdcProgressPhase.SNAPSHOT_WAITING_CHECKPOINT);
        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(context, splitAssigner);

        enumerator.registerReader(1);
        enumerator.registerReader(0);

        ArgumentCaptor<SourceEvent> event = ArgumentCaptor.forClass(SourceEvent.class);
        Mockito.verify(context, Mockito.times(2)).sendEventToSourceReader(Mockito.eq(0), event.capture());
        Mockito.verify(context, Mockito.never())
                .sendEventToSourceReader(Mockito.eq(1), Mockito.any(SourceEvent.class));
        for (SourceEvent value : event.getAllValues()) {
            assertEquals(
                    CdcProgressPhase.SNAPSHOT_WAITING_CHECKPOINT,
                    ((CdcProgressEvent) value).getPhase());
        }
    }
}

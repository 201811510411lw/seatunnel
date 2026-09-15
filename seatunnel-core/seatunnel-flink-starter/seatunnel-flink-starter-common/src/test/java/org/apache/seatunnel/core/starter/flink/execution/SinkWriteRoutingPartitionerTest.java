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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.api.sink.SinkWriteRouting;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SinkWriteRoutingPartitionerTest {

    @Test
    void shouldKeepOneSchemaControlRowPerSinkSubtask() {
        SinkWriteRouting<SeaTunnelRow> routing = mock(SinkWriteRouting.class);
        SinkWriteRoutingPartitioner.RoutingKeySelector selector =
                new SinkWriteRoutingPartitioner.RoutingKeySelector(routing, 2);
        SinkWriteRoutingPartitioner partitioner = new SinkWriteRoutingPartitioner();
        for (long subtask = 0; subtask < 2; subtask++) {
            SeaTunnelRow control = controlRow(subtask);
            assertEquals((int) subtask, partitioner.partition(selector.getKey(control), 2));
            assertEquals(subtask, control.getOptions().get("schema_subtask_id"));
        }
        verifyNoInteractions(routing);
    }

    @Test
    void shouldRejectSchemaControlRowsWithoutValidDestination() {
        SinkWriteRouting<SeaTunnelRow> routing = mock(SinkWriteRouting.class);
        SinkWriteRoutingPartitioner.RoutingKeySelector selector =
                new SinkWriteRoutingPartitioner.RoutingKeySelector(routing, 2);
        for (Object destination : new Object[] {null, -1L, 2L, "0"}) {
            assertThrows(
                    IllegalArgumentException.class, () -> selector.getKey(controlRow(destination)));
        }
        verifyNoInteractions(routing);
    }

    @Test
    void shouldApplyOwnershipPolicyToDataRows() {
        SinkWriteRouting<SeaTunnelRow> routing = mock(SinkWriteRouting.class);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {42});
        when(routing.route(row)).thenReturn(1);
        SinkWriteRoutingPartitioner.RoutingKeySelector selector =
                new SinkWriteRoutingPartitioner.RoutingKeySelector(routing, 2);
        assertEquals(1, new SinkWriteRoutingPartitioner().partition(selector.getKey(row), 2));
    }

    @Test
    void shouldRejectInvalidWriterOwnership() {
        SinkWriteRoutingPartitioner partitioner = new SinkWriteRoutingPartitioner();
        assertThrows(IllegalStateException.class, () -> partitioner.partition(-1, 2));
        assertThrows(IllegalStateException.class, () -> partitioner.partition(2, 2));
    }

    private SeaTunnelRow controlRow(Object destination) {
        SeaTunnelRow row = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put("schema_change_event", mock(SchemaChangeEvent.class));
        options.put("schema_subtask_id", destination);
        row.setOptions(options);
        return row;
    }
}

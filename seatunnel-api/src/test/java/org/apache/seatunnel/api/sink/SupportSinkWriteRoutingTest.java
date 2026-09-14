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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SupportSinkWriteRoutingTest {

    private static final SinkWriteRouting ROUTING =
            new SinkWriteRouting() {
                @Override
                public int route(SeaTunnelRow row, int numberOfWriters) {
                    return 0;
                }

                @Override
                public String targetIdentifier() {
                    return "test-target";
                }
            };

    @Test
    void shouldLeavePlainSinkUnrouted() {
        assertFalse(SupportSinkWriteRouting.resolve(new PlainSink()).isPresent());
    }

    @Test
    void shouldResolveRoutingCapability() {
        RoutingSink sink = new RoutingSink(Optional.of(ROUTING));
        assertSame(ROUTING, SupportSinkWriteRouting.resolve(sink).get());
    }

    @Test
    void shouldAllowCapabilityToDisableRouting() {
        assertFalse(SupportSinkWriteRouting.resolve(new RoutingSink(Optional.empty())).isPresent());
    }

    @Test
    void shouldResolveRoutingAfterSinkConfigurationChanges() {
        RoutingSink sink = new RoutingSink(Optional.empty());
        assertFalse(SupportSinkWriteRouting.resolve(sink).isPresent());
        sink.routing = Optional.of(ROUTING);
        assertSame(ROUTING, SupportSinkWriteRouting.resolve(sink).get());
    }

    @Test
    void shouldPropagateCapabilityValidationFailure() {
        IllegalStateException failure = new IllegalStateException("invalid routing configuration");
        RoutingSink sink =
                new RoutingSink(Optional.empty()) {
                    @Override
                    public Optional<SinkWriteRouting> getWriteRouting() {
                        throw failure;
                    }
                };
        assertSame(
                failure,
                assertThrows(
                        IllegalStateException.class, () -> SupportSinkWriteRouting.resolve(sink)));
    }

    private static class PlainSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {

        @Override
        public String getPluginName() {
            return "test-sink";
        }

        @Override
        public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
            throw new UnsupportedOperationException("This fixture only provides sink routing");
        }
    }

    private static class RoutingSink extends PlainSink implements SupportSinkWriteRouting {

        private Optional<SinkWriteRouting> routing;

        private RoutingSink(Optional<SinkWriteRouting> routing) {
            this.routing = routing;
        }

        @Override
        public Optional<SinkWriteRouting> getWriteRouting() {
            return routing;
        }
    }
}

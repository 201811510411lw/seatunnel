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

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SupportSinkWriteRoutingTest {

    private static final SinkWriteRouting<String> ROUTING =
            new SinkWriteRouting<String>() {
                @Override
                public int route(String record) {
                    return record.length() % 2;
                }

                @Override
                public String targetIdentifier() {
                    return "test-target";
                }
            };

    @Test
    void shouldLeavePlainSinkUnrouted() {
        assertFalse(SupportSinkWriteRouting.resolve(new PlainSink(), 2).isPresent());
    }

    @Test
    void shouldResolveRoutingCapability() {
        RoutingSink sink = new RoutingSink(Optional.of(ROUTING));
        SinkWriteRouting<String> routing = SupportSinkWriteRouting.resolve(sink, 2).get();
        assertSame(ROUTING, routing);
        assertEquals(1, routing.route("abc"));
    }

    @Test
    void shouldBindWriterCountWhenBuildingRouting() {
        RoutingSink sink =
                new RoutingSink(Optional.empty()) {
                    @Override
                    public Optional<SinkWriteRouting<String>> getWriteRouting(int writerCount) {
                        return Optional.of(
                                new SinkWriteRouting<String>() {
                                    @Override
                                    public int route(String record) {
                                        return record.length() % writerCount;
                                    }

                                    @Override
                                    public String targetIdentifier() {
                                        return "bound-target";
                                    }
                                });
                    }
                };
        SinkWriteRouting<String> twoWriters = SupportSinkWriteRouting.resolve(sink, 2).get();
        SinkWriteRouting<String> fourWriters = SupportSinkWriteRouting.resolve(sink, 4).get();
        assertEquals(1, twoWriters.route("abc"));
        assertEquals(3, fourWriters.route("abc"));
        assertEquals(1, twoWriters.route("abc"));
    }

    @Test
    void shouldAllowCapabilityToDisableRouting() {
        assertFalse(
                SupportSinkWriteRouting.resolve(new RoutingSink(Optional.empty()), 2).isPresent());
    }

    @Test
    void shouldResolveRoutingAfterSinkConfigurationChanges() {
        RoutingSink sink = new RoutingSink(Optional.empty());
        assertFalse(SupportSinkWriteRouting.resolve(sink, 2).isPresent());
        sink.routing = Optional.of(ROUTING);
        assertSame(ROUTING, SupportSinkWriteRouting.resolve(sink, 2).get());
    }

    @Test
    void shouldRejectNonPositiveWriterCount() {
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> SupportSinkWriteRouting.resolve(new PlainSink(), 0));
        assertEquals(
                "writerCount must be positive when resolving sink write routing: 0",
                failure.getMessage());
    }

    @Test
    void shouldPropagateCapabilityValidationFailure() {
        IllegalStateException failure = new IllegalStateException("invalid routing configuration");
        RoutingSink sink =
                new RoutingSink(Optional.empty()) {
                    @Override
                    public Optional<SinkWriteRouting<String>> getWriteRouting(int writerCount) {
                        throw failure;
                    }
                };
        assertSame(
                failure,
                assertThrows(
                        IllegalStateException.class,
                        () -> SupportSinkWriteRouting.resolve(sink, 2)));
    }

    private static class PlainSink implements SeaTunnelSink<String, Void, Void, Void> {

        @Override
        public String getPluginName() {
            return "test-sink";
        }

        @Override
        public SinkWriter<String, Void, Void> createWriter(SinkWriter.Context context) {
            throw new UnsupportedOperationException("This fixture only provides sink routing");
        }
    }

    private static class RoutingSink extends PlainSink implements SupportSinkWriteRouting<String> {

        private Optional<SinkWriteRouting<String>> routing;

        private RoutingSink(Optional<SinkWriteRouting<String>> routing) {
            this.routing = routing;
        }

        @Override
        public Optional<SinkWriteRouting<String>> getWriteRouting(int writerCount) {
            return routing;
        }
    }
}

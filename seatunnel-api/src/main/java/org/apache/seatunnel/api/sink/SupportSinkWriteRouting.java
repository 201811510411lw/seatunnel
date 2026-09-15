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

import java.util.Optional;

/** Optional sink capability for routing records to the writer that owns their physical target. */
public interface SupportSinkWriteRouting<T> {

    /**
     * Returns the routing policy, or empty when this sink uses the engine's default routing.
     *
     * <p>Called after loading the target tables and before creating or restoring writers. An
     * implementation may initialize writer ownership for the given writer count here and must
     * reject unsupported layouts instead of silently falling back to default routing. Wrappers must
     * expose their delegates' routing policies and validate that they can be combined safely.
     */
    Optional<SinkWriteRouting<T>> getWriteRouting(int writerCount);

    /**
     * Resolves routing during sink setup or lifecycle handling, not for each input record.
     *
     * <p>Implementing the capability alone does not mean routing is enabled: a wrapper containing
     * only sinks with default routing can return empty. Initialization failures propagate to the
     * caller so that a required ownership policy cannot be bypassed.
     */
    static <T> Optional<SinkWriteRouting<T>> resolve(
            SeaTunnelSink<T, ?, ?, ?> sink, int writerCount) {
        if (writerCount <= 0) {
            throw new IllegalArgumentException(
                    "writerCount must be positive when resolving sink write routing: "
                            + writerCount);
        }
        return sink instanceof SupportSinkWriteRouting
                ? ((SupportSinkWriteRouting<T>) sink).getWriteRouting(writerCount)
                : Optional.empty();
    }
}

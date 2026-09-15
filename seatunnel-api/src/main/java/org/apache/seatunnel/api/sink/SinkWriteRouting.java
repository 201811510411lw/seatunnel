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

import java.io.Serializable;

/**
 * Serializable writer ownership policy bound to a writer count during sink setup.
 *
 * <p>A task uses its policy on one thread. Implementations may reuse local extraction state but
 * must not retain or mutate input records. Control events must be handled by the engine separately.
 *
 * @param <T> the sink's input record type
 */
public interface SinkWriteRouting<T> extends Serializable {

    /** Returns the owning writer index in {@code [0, writerCount)} for this record. */
    int route(T record);

    /** Identifies the physical target so wrappers can reject conflicting independent writers. */
    String targetIdentifier();
}

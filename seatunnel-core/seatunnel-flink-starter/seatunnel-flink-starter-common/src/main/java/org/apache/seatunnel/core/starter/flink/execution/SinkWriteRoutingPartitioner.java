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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.Map;

/** Applies writer ownership while preserving each schema control message's downstream subtask. */
public final class SinkWriteRoutingPartitioner implements Partitioner<Integer> {

    private static final long serialVersionUID = 1L;

    @Override
    public int partition(Integer writer, int numberOfPartitions) {
        if (writer == null || writer < 0 || writer >= numberOfPartitions) {
            throw new IllegalStateException(
                    "Sink routing returned an invalid writer index: " + writer);
        }
        return writer;
    }

    public static final class RoutingKeySelector implements KeySelector<SeaTunnelRow, Integer> {

        private static final long serialVersionUID = 1L;

        private final SinkWriteRouting<SeaTunnelRow> routing;
        private final int parallelism;

        public RoutingKeySelector(SinkWriteRouting<SeaTunnelRow> routing, int parallelism) {
            if (parallelism <= 0) {
                throw new IllegalArgumentException("Sink writer parallelism must be positive");
            }
            this.routing = routing;
            this.parallelism = parallelism;
        }

        @Override
        public Integer getKey(SeaTunnelRow row) {
            Map<String, Object> options = row.getOptions();
            if (options != null && options.containsKey("schema_change_event")) {
                // BroadcastSchemaSinkOperator emits zero-field rows, one for each sink subtask.
                // Preserve that destination so the writer can apply the event and acknowledge it.
                Object destination = options.get("schema_subtask_id");
                if (!(destination instanceof Long)
                        || (Long) destination < 0
                        || (Long) destination >= parallelism) {
                    throw new IllegalArgumentException(
                            "Schema control row has an invalid schema_subtask_id: " + destination);
                }
                return ((Long) destination).intValue();
            }
            return routing.route(row);
        }
    }
}

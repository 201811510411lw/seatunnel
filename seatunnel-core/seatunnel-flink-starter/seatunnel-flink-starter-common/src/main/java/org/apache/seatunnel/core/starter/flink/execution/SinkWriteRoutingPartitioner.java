package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.api.sink.SinkWriteRouting;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;

public final class SinkWriteRoutingPartitioner implements Partitioner<Integer> {

    @Override
    public int partition(Integer writer, int numberOfPartitions) {
        if (writer < 0 || writer >= numberOfPartitions) {
            throw new IllegalStateException("Sink routing returned an invalid writer index");
        }
        return writer;
    }

    public static final class RoutingKeySelector implements KeySelector<SeaTunnelRow, Integer> {

        private final SinkWriteRouting routing;
        private final int parallelism;

        public RoutingKeySelector(SinkWriteRouting routing, int parallelism) {
            this.routing = routing;
            this.parallelism = parallelism;
        }

        @Override
        public Integer getKey(SeaTunnelRow row) {
            return routing.route(row, parallelism);
        }
    }
}

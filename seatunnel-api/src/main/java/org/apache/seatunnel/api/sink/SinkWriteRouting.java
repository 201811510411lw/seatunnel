package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.Serializable;

public interface SinkWriteRouting extends Serializable {

    int route(SeaTunnelRow row, int numberOfWriters);

    String targetIdentifier();
}

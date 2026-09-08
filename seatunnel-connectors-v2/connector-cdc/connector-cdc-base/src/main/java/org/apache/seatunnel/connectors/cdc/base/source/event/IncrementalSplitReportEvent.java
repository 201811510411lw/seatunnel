package org.apache.seatunnel.connectors.cdc.base.source.event;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class IncrementalSplitReportEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;

    private final List<IncrementalSplit> splits;
}

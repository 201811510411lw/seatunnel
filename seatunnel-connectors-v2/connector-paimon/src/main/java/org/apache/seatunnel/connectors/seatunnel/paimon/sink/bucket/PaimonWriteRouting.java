package org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket;

import org.apache.seatunnel.api.sink.SinkWriteRouting;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowConverter;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;

public final class PaimonWriteRouting implements SinkWriteRouting {

    private final TableSchema schema;
    private final SeaTunnelRowType rowType;
    private final String targetIdentifier;
    private transient FixedBucketRowKeyExtractor extractor;

    public PaimonWriteRouting(FileStoreTable table, SeaTunnelRowType rowType) {
        if (table.bucketMode() != BucketMode.HASH_FIXED) {
            throw new UnsupportedOperationException(
                    "Flink Paimon bucket routing requires a fixed-bucket table; dynamic, "
                            + "cross-partition and bucket-unaware modes are not supported");
        }
        this.schema = table.schema();
        this.rowType = rowType;
        this.targetIdentifier = table.location().toString();
    }

    @Override
    public int route(SeaTunnelRow row, int numberOfWriters) {
        if (numberOfWriters < 1) {
            throw new IllegalArgumentException("Paimon writer parallelism must be positive");
        }
        if (row.getOptions() != null
                && (row.getOptions().containsKey("schema_change_event")
                        || row.getOptions().containsKey("flush_event"))) {
            throw new UnsupportedOperationException(
                    "Paimon bucket routing does not support online schema evolution; "
                            + "stop the job and rebuild with the new schema");
        }
        if (extractor == null) {
            extractor = new FixedBucketRowKeyExtractor(schema);
        }
        extractor.setRecord(RowConverter.reconvert(row, rowType, schema));
        return ChannelComputer.select(extractor.partition(), extractor.bucket(), numberOfWriters);
    }

    @Override
    public String targetIdentifier() {
        return targetIdentifier;
    }
}

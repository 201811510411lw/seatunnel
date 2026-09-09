package org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket;

import org.apache.seatunnel.api.sink.SinkWriteRouting;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowConverter;

import org.apache.paimon.data.InternalRow;
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
        validateRoutingRequest(row, numberOfWriters);
        if (numberOfWriters == 1) {
            return 0;
        }
        return selectWriter(RowConverter.reconvert(row, rowType, schema), numberOfWriters);
    }

    public InternalRow convertForWriter(SeaTunnelRow row, int numberOfWriters, int writerIndex) {
        validateRoutingRequest(row, numberOfWriters);
        if (row.getArity() != rowType.getTotalFields()) {
            throw new IllegalArgumentException("Paimon row field count does not match its schema");
        }
        InternalRow converted = RowConverter.reconvert(row, rowType, schema);
        if (selectWriter(converted, numberOfWriters) != writerIndex) {
            throw new IllegalStateException(
                    "Paimon row reached a writer that does not own its bucket");
        }
        return converted;
    }

    private void validateRoutingRequest(SeaTunnelRow row, int numberOfWriters) {
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
    }

    private int selectWriter(InternalRow row, int numberOfWriters) {
        if (numberOfWriters == 1) {
            return 0;
        }
        if (extractor == null) {
            extractor = new FixedBucketRowKeyExtractor(schema);
        }
        extractor.setRecord(row);
        return ChannelComputer.select(extractor.partition(), extractor.bucket(), numberOfWriters);
    }

    @Override
    public String targetIdentifier() {
        return targetIdentifier;
    }
}

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

/** Fixed-bucket ownership derived from the loaded Paimon physical schema. */
public final class PaimonWriteRouting implements SinkWriteRouting<SeaTunnelRow> {

    private final TableSchema schema;
    private final SeaTunnelRowType rowType;
    private final String targetIdentifier;
    private final int numberOfWriters;
    private transient FixedBucketRowKeyExtractor extractor;

    /** Captures the immutable routing schema and physical target identity for serialization. */
    public PaimonWriteRouting(FileStoreTable table, SeaTunnelRowType rowType, int numberOfWriters) {
        if (numberOfWriters < 1) {
            throw new IllegalArgumentException("Paimon writer parallelism must be positive");
        }
        if (table.bucketMode() != BucketMode.HASH_FIXED) {
            throw new UnsupportedOperationException(
                    "Flink Paimon bucket routing requires a fixed-bucket table; dynamic, "
                            + "cross-partition and bucket-unaware modes are not supported");
        }
        this.schema = table.schema();
        this.rowType = rowType;
        this.targetIdentifier = table.location().toString();
        this.numberOfWriters = numberOfWriters;
    }

    /** Validates the bound policy against the writer context before opening the table writer. */
    public void validateWriterContext(int parallelism, int writerIndex) {
        if (parallelism != numberOfWriters || writerIndex < 0 || writerIndex >= numberOfWriters) {
            throw new IllegalArgumentException(
                    "Paimon writer context does not match its bound routing policy; "
                            + "routingParallelism="
                            + numberOfWriters
                            + ", writerParallelism="
                            + parallelism
                            + ", writerIndex="
                            + writerIndex);
        }
    }

    /** Selects one writer for each physical partition/bucket independently of the row kind. */
    @Override
    public int route(SeaTunnelRow row) {
        validateRoutingRequest(row);
        if (numberOfWriters == 1) {
            return 0;
        }
        return selectWriter(RowConverter.reconvert(row, rowType, schema));
    }

    /** Converts a row once for writing and verifies that this writer owns its partition/bucket. */
    public InternalRow convertForWriter(SeaTunnelRow row, int writerIndex) {
        validateRoutingRequest(row);
        if (row.getArity() != rowType.getTotalFields()) {
            throw new IllegalArgumentException("Paimon row field count does not match its schema");
        }
        InternalRow converted = RowConverter.reconvert(row, rowType, schema);
        if (selectWriter(converted) != writerIndex) {
            throw new IllegalStateException(
                    "Paimon row reached a writer that does not own its bucket");
        }
        return converted;
    }

    private void validateRoutingRequest(SeaTunnelRow row) {
        if (row.getOptions() != null
                && (row.getOptions().containsKey("schema_change_event")
                        || row.getOptions().containsKey("flush_event"))) {
            throw new UnsupportedOperationException(
                    "Paimon bucket routing does not support online schema evolution; "
                            + "stop the job and rebuild with the new schema");
        }
    }

    /** Reuses the extractor within one routing instance without retaining converted records. */
    private int selectWriter(InternalRow row) {
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

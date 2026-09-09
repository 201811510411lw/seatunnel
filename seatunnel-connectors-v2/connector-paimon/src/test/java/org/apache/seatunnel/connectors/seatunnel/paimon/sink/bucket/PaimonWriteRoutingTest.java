package org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.sink.SinkWriteRouting;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowConverter;

import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonWriteRoutingTest {

    @Test
    void shouldMatchPhysicalPartitionAndCustomBucketKeyAcrossSerialization() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "8");
        options.put("bucket-key", "tenant");
        TableSchema schema =
                new TableSchema(
                        0L,
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT().notNull()),
                                new DataField(1, "tenant", DataTypes.STRING().notNull()),
                                new DataField(2, "part", DataTypes.STRING().notNull())),
                        2,
                        Collections.singletonList("part"),
                        Arrays.asList("id", "tenant", "part"),
                        options,
                        "routing test");
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "tenant", "part"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(table.schema()).thenReturn(schema);
        when(table.location()).thenReturn(new Path("file:/synthetic/routing"));
        SinkWriteRouting routing = new PaimonWriteRouting(table, rowType);
        DefaultSerializer<SinkWriteRouting> serializer = new DefaultSerializer<>();
        SinkWriteRouting restored = serializer.deserialize(serializer.serialize(routing));
        FixedBucketRowKeyExtractor extractor = new FixedBucketRowKeyExtractor(schema);
        Map<String, Integer> owners = new HashMap<>();
        Set<Integer> usedWriters = new HashSet<>();
        for (int identifier = 0; identifier < 200; identifier++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                identifier, "tenant-" + identifier % 17, "part-" + identifier % 3
                            });
            extractor.setRecord(RowConverter.reconvert(row, rowType, schema));
            int owner = routing.route(row, 4);
            assertEquals(
                    ChannelComputer.select(extractor.partition(), extractor.bucket(), 4), owner);
            assertEquals(owner, restored.route(row, 4));
            String bucket = row.getField(2) + ":" + extractor.bucket();
            Integer previous = owners.put(bucket, owner);
            if (previous != null) {
                assertEquals(previous.intValue(), owner);
            }
            row.setRowKind(RowKind.DELETE);
            assertEquals(owner, routing.route(row, 4));
            usedWriters.add(owner);
        }
        assertTrue(usedWriters.size() > 1);
    }

    @Test
    void shouldRejectUnsupportedBucketModes() {
        for (BucketMode mode :
                Arrays.asList(
                        BucketMode.HASH_DYNAMIC,
                        BucketMode.CROSS_PARTITION,
                        BucketMode.BUCKET_UNAWARE)) {
            FileStoreTable table = mock(FileStoreTable.class);
            when(table.bucketMode()).thenReturn(mode);
            assertThrows(
                    UnsupportedOperationException.class, () -> new PaimonWriteRouting(table, null));
        }
    }
}

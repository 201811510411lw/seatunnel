package org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonWriteRoutingConversionTest {

    @Test
    void shouldRouteSingleWriterWithoutDecodingButRejectInvalidPayloadBeforeWriting() {
        PaimonWriteRouting routing = createRouting();
        SeaTunnelRow incomplete = new SeaTunnelRow(new Object[] {17});

        assertEquals(0, routing.route(incomplete, 1));
        assertThrows(
                IllegalArgumentException.class, () -> routing.convertForWriter(incomplete, 1, 0));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void shouldRejectExtraFieldsRatherThanSilentlyDiscardingThem(int parallelism) {
        PaimonWriteRouting routing = createRouting();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {17, "tenant-a", "part-a", "extra"});

        assertThrows(
                IllegalArgumentException.class,
                () -> routing.convertForWriter(row, parallelism, 0));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void shouldReturnWritableRowForItsOwner(int parallelism) {
        PaimonWriteRouting routing = createRouting();
        for (RowKind rowKind : RowKind.values()) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {17, "tenant-a", "part-a"});
            row.setRowKind(rowKind);
            int owner = routing.route(row, parallelism);

            InternalRow converted = routing.convertForWriter(row, parallelism, owner);

            assertEquals(17, converted.getInt(0));
            assertEquals("tenant-a", converted.getString(1).toString());
            assertEquals("part-a", converted.getString(2).toString());
            assertEquals(rowKind.name(), converted.getRowKind().name());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void shouldRejectWrongWriterAndOutOfRangeWriter(int parallelism) {
        PaimonWriteRouting routing = createRouting();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {17, "tenant-a", "part-a"});
        int owner = routing.route(row, parallelism);

        assertThrows(
                IllegalStateException.class, () -> routing.convertForWriter(row, parallelism, -1));
        assertThrows(
                IllegalStateException.class,
                () -> routing.convertForWriter(row, parallelism, parallelism));
        if (parallelism > 1) {
            assertThrows(
                    IllegalStateException.class,
                    () -> routing.convertForWriter(row, parallelism, (owner + 1) % parallelism));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void shouldRejectControlEventsBeforeDecoding(int parallelism) {
        PaimonWriteRouting routing = createRouting();
        for (String eventName : Arrays.asList("schema_change_event", "flush_event")) {
            SeaTunnelRow event = new SeaTunnelRow(new Object[0]);
            event.setOptions(Collections.singletonMap(eventName, "synthetic"));
            assertThrows(
                    UnsupportedOperationException.class, () -> routing.route(event, parallelism));
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> routing.convertForWriter(event, parallelism, 0));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void shouldRejectInvalidParallelismBeforeDecoding(int parallelism) {
        PaimonWriteRouting routing = createRouting();
        SeaTunnelRow incomplete = new SeaTunnelRow(new Object[0]);
        assertThrows(IllegalArgumentException.class, () -> routing.route(incomplete, parallelism));
        assertThrows(
                IllegalArgumentException.class,
                () -> routing.convertForWriter(incomplete, parallelism, 0));
    }

    @Test
    void shouldNotOverwritePreviouslyConvertedRows() {
        PaimonWriteRouting routing = createRouting();
        InternalRow first =
                routing.convertForWriter(
                        new SeaTunnelRow(new Object[] {17, "tenant-a", "part-a"}), 1, 0);
        routing.convertForWriter(new SeaTunnelRow(new Object[] {18, "tenant-b", "part-b"}), 1, 0);

        assertEquals(17, first.getInt(0));
        assertEquals("tenant-a", first.getString(1).toString());
        assertEquals("part-a", first.getString(2).toString());
    }

    private PaimonWriteRouting createRouting() {
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
                        "conversion test");
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "tenant", "part"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(table.schema()).thenReturn(schema);
        when(table.location()).thenReturn(new Path("file:/synthetic/conversion"));
        return new PaimonWriteRouting(table, rowType);
    }
}

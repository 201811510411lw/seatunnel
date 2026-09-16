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
        PaimonWriteRouting routing = createRouting(1);
        SeaTunnelRow incomplete = new SeaTunnelRow(new Object[] {17});

        assertEquals(0, routing.select(incomplete));
        assertThrows(IllegalArgumentException.class, () -> routing.convertForWriter(incomplete, 0));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void shouldRejectExtraFieldsRatherThanSilentlyDiscardingThem(int parallelism) {
        PaimonWriteRouting routing = createRouting(parallelism);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {17, "tenant-a", "part-a", "extra"});

        assertThrows(IllegalArgumentException.class, () -> routing.convertForWriter(row, 0));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void shouldReturnWritableRowForItsOwner(int parallelism) {
        PaimonWriteRouting routing = createRouting(parallelism);
        for (RowKind rowKind : RowKind.values()) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {17, "tenant-a", "part-a"});
            row.setRowKind(rowKind);
            int owner = routing.select(row);

            InternalRow converted = routing.convertForWriter(row, owner);

            assertEquals(17, converted.getInt(0));
            assertEquals("tenant-a", converted.getString(1).toString());
            assertEquals("part-a", converted.getString(2).toString());
            assertEquals(rowKind.name(), converted.getRowKind().name());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void shouldRejectWrongWriterAndOutOfRangeWriter(int parallelism) {
        PaimonWriteRouting routing = createRouting(parallelism);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {17, "tenant-a", "part-a"});
        int owner = routing.select(row);

        assertThrows(IllegalStateException.class, () -> routing.convertForWriter(row, -1));
        assertThrows(IllegalStateException.class, () -> routing.convertForWriter(row, parallelism));
        if (parallelism > 1) {
            assertThrows(
                    IllegalStateException.class,
                    () -> routing.convertForWriter(row, (owner + 1) % parallelism));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void shouldRejectControlEventsBeforeDecoding(int parallelism) {
        PaimonWriteRouting routing = createRouting(parallelism);
        for (String eventName : Arrays.asList("schema_change_event", "flush_event")) {
            SeaTunnelRow event = new SeaTunnelRow(new Object[0]);
            event.setOptions(Collections.singletonMap(eventName, "synthetic"));
            assertThrows(UnsupportedOperationException.class, () -> routing.select(event));
            assertThrows(
                    UnsupportedOperationException.class, () -> routing.convertForWriter(event, 0));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void shouldRejectInvalidParallelismAtConstruction(int parallelism) {
        assertThrows(IllegalArgumentException.class, () -> createRouting(parallelism));
    }

    @Test
    void shouldRejectWriterContextThatDiffersFromBoundRouting() {
        PaimonWriteRouting routing = createRouting(2);
        routing.validateWriterContext(2, 0);
        routing.validateWriterContext(2, 1);
        assertThrows(IllegalArgumentException.class, () -> routing.validateWriterContext(1, 0));
        assertThrows(IllegalArgumentException.class, () -> routing.validateWriterContext(2, -1));
        assertThrows(IllegalArgumentException.class, () -> routing.validateWriterContext(2, 2));
    }

    @Test
    void shouldNotOverwritePreviouslyConvertedRows() {
        PaimonWriteRouting routing = createRouting(1);
        InternalRow first =
                routing.convertForWriter(
                        new SeaTunnelRow(new Object[] {17, "tenant-a", "part-a"}), 0);
        routing.convertForWriter(new SeaTunnelRow(new Object[] {18, "tenant-b", "part-b"}), 0);

        assertEquals(17, first.getInt(0));
        assertEquals("tenant-a", first.getString(1).toString());
        assertEquals("part-a", first.getString(2).toString());
    }

    private PaimonWriteRouting createRouting(int parallelism) {
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
        return new PaimonWriteRouting(table, rowType, parallelism);
    }
}

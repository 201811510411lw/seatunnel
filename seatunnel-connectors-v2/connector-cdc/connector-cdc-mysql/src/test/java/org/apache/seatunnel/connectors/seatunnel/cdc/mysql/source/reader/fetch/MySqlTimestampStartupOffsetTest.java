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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch;

import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MySqlTimestampStartupOffsetTest {

    @Test
    public void testFreshTimestampStartResolvesConfiguredTimestamp() {
        BinlogOffset timestampOffset = new BinlogOffset(1716076800L);

        assertTrue(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, timestampOffset));
        assertFalse(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.EARLIEST, timestampOffset));
        assertFalse(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.SPECIFIC, timestampOffset));
    }

    @Test
    public void testCheckpointRestoreUsesPersistedOffset() {
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split",
                        Collections.singletonList(TableId.parse("test.orders")),
                        new BinlogOffset(1716076800L),
                        BinlogOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList());
        IncrementalSplitState splitState = new IncrementalSplitState(split);
        BinlogOffset checkpointOffset = new BinlogOffset("mysql-bin.000021", 4096L);
        splitState.setStartupOffset(checkpointOffset);

        IncrementalSplit restoredSplit = splitState.toSourceSplit();

        assertEquals(checkpointOffset, restoredSplit.getStartupOffset());
        assertFalse(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, restoredSplit.getStartupOffset()));
    }

    @Test
    public void testSavepointRestoreKeepsTransactionOffset() throws Exception {
        BinlogOffset transactionOffset = transactionOffset();
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split",
                        Collections.singletonList(TableId.parse("test.orders")),
                        transactionOffset,
                        BinlogOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList());

        IncrementalSplit restoredSplit = roundTrip(split);

        assertEquals(transactionOffset, restoredSplit.getStartupOffset());
        assertEquals("mysql-bin.035519", transactionOffset.getFilename());
        assertEquals(215029010L, transactionOffset.getPosition());
        assertEquals(31L, transactionOffset.getRestartSkipEvents());
        assertEquals(9L, transactionOffset.getRestartSkipRows());
        assertFalse(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, restoredSplit.getStartupOffset()));
    }

    private static BinlogOffset transactionOffset() {
        Map<String, String> offset = new LinkedHashMap<>();
        offset.put("file", "mysql-bin.035519");
        offset.put("pos", "215029010");
        offset.put("event", "31");
        offset.put("row", "9");
        offset.put("ts_sec", "1793602827");
        offset.put("gtids", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5");
        offset.put("server_id", "42");
        return new BinlogOffset(offset);
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) input.readObject();
        }
    }
}

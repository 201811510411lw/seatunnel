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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Restores durable state produced by Flink's original global committer into the recovery adapter.
 */
class PaimonFlinkLegacyRecoveryTest {

    private static final int WRITERS = 4;
    private static final int ROWS = 128;

    @TempDir private Path temporaryDirectory;

    @Test
    @Timeout(180)
    void shouldRestoreLegacyCheckpointWithUnemittedLocalCommits() throws Exception {
        String runId = UUID.randomUUID().toString();
        PaimonFlinkPendingRecoveryTest.registerFailureControl(runId, WRITERS);
        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", temporaryDirectory.resolve("warehouse").toString());
        properties.put("plugin_name", "Paimon");
        properties.put("database", "legacy_recovery");
        properties.put("table", "pending_rows");
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "16");
        options.put("write-only", "true");
        options.put("commit.timeout", "5 s");
        properties.put("paimon.table.write-props", options);
        ReadonlyConfig config = ReadonlyConfig.fromMap(properties);
        TablePath tablePath = TablePath.of("legacy_recovery", "pending_rows");
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("paimon", "legacy_recovery", "pending_rows"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id",
                                                BasicType.INT_TYPE,
                                                (Long) null,
                                                false,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "value",
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null))
                                .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "legacy checkpoint recovery regression");
        PaimonCatalog catalog = new PaimonCatalog("paimon", config);
        catalog.open();
        JobClient legacy = null;
        JobClient restored = null;
        try {
            catalog.createDatabase(tablePath, true);
            catalog.createTable(tablePath, table, false);
            Path checkpoints = temporaryDirectory.resolve("checkpoints");
            legacy = submit(config, table, runId, checkpoints, null);
            Future<?> legacyResult = legacy.getJobExecutionResult();
            assertThrows(ExecutionException.class, () -> legacyResult.get(60, TimeUnit.SECONDS));
            assertTrue(
                    PaimonFlinkPendingRecoveryTest.wasFailureInjected(runId),
                    "The legacy job must fail before local commit emission");
            assertEquals(WRITERS, PaimonFlinkPendingRecoveryTest.failureFragments(runId));
            assertEquals(1, PaimonFlinkPendingRecoveryTest.failureCommitBoundaries(runId).size());
            Path checkpoint = latestCompletedCheckpoint(checkpoints);
            assertTrue(
                    PaimonFlinkPendingRecoveryTest.readRows(
                                    (FileStoreTable) catalog.getPaimonTable(tablePath))
                            .isEmpty(),
                    "Legacy pending rows must not already be globally committed");

            restored = submit(config, table, runId, checkpoints, checkpoint);
            Map<Integer, String> expected = new LinkedHashMap<>();
            for (int id = 0; id < ROWS; id++) {
                expected.put(id, "pending-" + id);
            }
            expected.put(0, "after-recovery");
            expected.remove(1);
            expected.put(ROWS, "inserted-after-recovery");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(75);
            Map<Integer, String> actual = Collections.emptyMap();
            while (System.nanoTime() < deadline) {
                if (restored.getJobExecutionResult().isDone()) {
                    restored.getJobExecutionResult().get(5, TimeUnit.SECONDS);
                }
                actual =
                        PaimonFlinkPendingRecoveryTest.readRows(
                                (FileStoreTable) catalog.getPaimonTable(tablePath));
                if (PaimonFlinkPendingRecoveryTest.completedAfterRestore(runId) >= 3
                        && expected.equals(actual)) {
                    break;
                }
                Thread.sleep(100);
            }
            assertEquals(
                    checkpointId(checkpoint),
                    PaimonFlinkPendingRecoveryTest.restoredCheckpoint(runId));
            assertTrue(
                    PaimonFlinkPendingRecoveryTest.completedAfterRestore(runId) >= 3,
                    "Legacy state must permit new checkpoints");
            assertEquals(
                    expected, actual, "All legacy fragments and subsequent I/U/D must survive");
        } finally {
            if (restored != null && !restored.getJobExecutionResult().isDone()) {
                restored.cancel().get(20, TimeUnit.SECONDS);
            }
            if (legacy != null && !legacy.getJobExecutionResult().isDone()) {
                legacy.cancel().get(20, TimeUnit.SECONDS);
            }
            catalog.close();
            PaimonFlinkPendingRecoveryTest.removeFailureControl(runId);
        }
    }

    private static JobClient submit(
            ReadonlyConfig config, CatalogTable table, String runId, Path checkpoints, Path restore)
            throws Exception {
        Configuration flinkConfig = new Configuration();
        if (restore != null) {
            flinkConfig.setString("execution.savepoint.path", restore.toUri().toString());
        }
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(WRITERS, flinkConfig);
        environment.getConfig().disableClosureCleaner();
        environment.enableCheckpointing(500);
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        environment.getCheckpointConfig().setCheckpointStorage(checkpoints.toUri().toString());
        environment
                .getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        environment.setRestartStrategy(RestartStrategies.noRestart());
        JobContext jobContext = new JobContext(118L);
        jobContext.setJobMode(JobMode.STREAMING);
        jobContext.setEnableCheckpoint(true);
        PaimonSink sink = PaimonFlinkPendingRecoveryTest.failingSink(config, table, runId);
        sink.setJobContext(jobContext);
        DataStream<SeaTunnelRow> input =
                environment
                        .addSource(PaimonFlinkPendingRecoveryTest.pendingSource(runId))
                        .uid("pending-source")
                        .setParallelism(1);
        if (restore == null) {
            PaimonFlinkPendingRecoveryTest.legacyRoutedSink(
                            input, sink, Collections.singletonList(table), WRITERS)
                    .uid("pending-sink");
        } else {
            new SinkExecuteProcessor(
                            new ArrayList<>(),
                            ConfigFactory.parseString(
                                    "paimon.legacy-state.writer-parallelism = "
                                            + WRITERS
                                            + "\npaimon.legacy-state.checkpoint-id = "
                                            + checkpointId(restore)),
                            Collections.emptyList(),
                            jobContext)
                    .createVersionSpecificDataStreamSink(
                            new DataStreamTableInfo(
                                    input, Collections.singletonList(table), "input"),
                            sink,
                            WRITERS,
                            ConfigFactory.empty())
                    .uid("pending-sink");
        }
        return environment.executeAsync("paimon-legacy-pending-recovery");
    }

    private static Path latestCompletedCheckpoint(Path directory) throws Exception {
        try (Stream<Path> paths = Files.walk(directory)) {
            return paths.filter(path -> path.getFileName().toString().equals("_metadata"))
                    .max(Comparator.comparingLong(PaimonFlinkLegacyRecoveryTest::checkpointId))
                    .orElseThrow(
                            () ->
                                    new AssertionError(
                                            "The legacy job must retain a completed checkpoint"));
        }
    }

    private static long checkpointId(Path metadata) {
        return Long.parseLong(
                metadata.getParent().getFileName().toString().substring("chk-".length()));
    }
}

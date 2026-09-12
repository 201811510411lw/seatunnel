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
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.MySqlIncrementalSource;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.MySqlIncrementalSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink;
import org.apache.seatunnel.translation.flink.source.FlinkSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.DataInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in external MySQL integration. The private properties file contains host, port, database,
 * username and password. Only UUID-prefixed tables created by this run are written. Source tables
 * and the local warehouse are retained for inspection; this test does not drop database objects.
 */
@EnabledIfSystemProperty(named = "cf.mysql.acceptance.config", matches = ".+")
@SuppressWarnings({"rawtypes", "unchecked"})
class PaimonMySqlCdcRecoveryIT {

    private static final Map<String, Progress> RUNS = new ConcurrentHashMap<>();

    @Test
    @Timeout(600)
    void shouldSnapshotStreamAndRecoverRealMySql() throws Exception {
        Properties connectionProperties = loadPrivateProperties();
        String database = required(connectionProperties, "database");
        String username = required(connectionProperties, "username");
        String password = required(connectionProperties, "password");
        String jdbcUrl =
                "jdbc:mysql://"
                        + required(connectionProperties, "host")
                        + ":"
                        + connectionProperties.getProperty("port", "3306")
                        + "/"
                        + database
                        + "?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8"
                        + "&serverTimezone=UTC&rewriteBatchedStatements=true";
        int rows = positiveProperty("cf.mysql.acceptance.rows", 20000);
        assertTrue(rows >= 4, "At least four rows are needed for distinct DELETE probes");
        int parallelism = positiveProperty("cf.mysql.acceptance.parallelism", 4);
        int splitSize = positiveProperty("cf.mysql.acceptance.splitSize", 1024);
        int payloadSize = positiveProperty("cf.mysql.acceptance.payload", 3000);
        assertTrue(payloadSize <= 4000, "payload must fit the isolated table column");
        boolean fail = Boolean.parseBoolean(System.getProperty("cf.mysql.acceptance.fail", "true"));
        boolean legacy =
                Boolean.parseBoolean(System.getProperty("cf.mysql.acceptance.legacy", "false"));
        boolean savepointEnabled =
                Boolean.parseBoolean(System.getProperty("cf.mysql.acceptance.savepoint", "true"));
        String runId =
                "cf_recovery_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        List<String> tableNames = Arrays.asList(runId + "_narrow", runId + "_wide");
        Path output =
                Paths.get(
                                System.getProperty(
                                        "cf.mysql.acceptance.output", "target/mysql-acceptance"))
                        .toAbsolutePath()
                        .resolve(runId);
        Files.createDirectories(output);
        Properties evidence = new Properties();
        evidence.setProperty("runId", runId);
        evidence.setProperty("tables", String.join(",", tableNames));
        evidence.setProperty("rowsPerTable", String.valueOf(rows));
        evidence.setProperty("parallelism", String.valueOf(parallelism));
        evidence.setProperty("splitSize", String.valueOf(splitSize));
        evidence.setProperty("fetchSize", "1024");
        evidence.setProperty("payload", String.valueOf(payloadSize));
        evidence.setProperty(
                "checkpointMillis",
                String.valueOf(positiveProperty("cf.mysql.acceptance.checkpointMillis", 1000)));
        evidence.setProperty("failureEnabled", String.valueOf(fail));
        evidence.setProperty("legacyTopology", String.valueOf(legacy));
        evidence.setProperty("savepointEnabled", String.valueOf(savepointEnabled));
        evidence.setProperty("warehouse", output.resolve("warehouse").toString());
        writeCleanupSql(output, database, tableNames);
        Progress progress = new Progress();
        RUNS.put(runId, progress);
        if (fail) {
            PaimonFlinkPendingRecoveryTest.registerFailureControl(runId, parallelism);
        }
        Class.forName("com.mysql.cj.jdbc.Driver");
        JobClient client = null;
        List<PaimonCatalog> catalogs = new ArrayList<>();
        try (Connection jdbc = DriverManager.getConnection(jdbcUrl, username, password)) {
            verifyBinlog(jdbc);
            Map<String, Map<Long, List<Object>>> expected = new LinkedHashMap<>();
            for (int index = 0; index < tableNames.size(); index++) {
                String name = tableNames.get(index);
                createAndFill(jdbc, name, rows, index == 0 ? 16 : payloadSize);
                expected.put(name, readMySql(jdbc, name));
            }
            evidence.setProperty("sourceTablesCreated", "true");
            writeEvidence(output, evidence);

            Map<String, Object> sourceOptions = new HashMap<>();
            sourceOptions.put("plugin_name", "MySQL-CDC");
            sourceOptions.put("url", jdbcUrl);
            sourceOptions.put("username", username);
            sourceOptions.put("password", password);
            sourceOptions.put(
                    "table-names",
                    Arrays.asList(
                            database + "." + tableNames.get(0),
                            database + "." + tableNames.get(1)));
            sourceOptions.put("database-names", Collections.singletonList(database));
            sourceOptions.put("startup.mode", "INITIAL");
            sourceOptions.put("exactly_once", true);
            sourceOptions.put("schema-changes.enabled", false);
            sourceOptions.put("snapshot.split.size", splitSize);
            sourceOptions.put("snapshot.fetch.size", 1024);
            sourceOptions.put("server-time-zone", "UTC");
            int serverId =
                    Integer.getInteger(
                            "cf.mysql.acceptance.serverId",
                            ThreadLocalRandom.current()
                                    .nextInt(100000000, 2000000000 - parallelism - 1));
            sourceOptions.put("server-id", serverId + "-" + (serverId + parallelism + 1));
            SeaTunnelSource source =
                    new MySqlIncrementalSourceFactory()
                            .createSource(
                                    new TableSourceFactoryContext(
                                            ReadonlyConfig.fromMap(sourceOptions),
                                            getClass().getClassLoader()))
                            .createSource();
            assertTrue(source instanceof MySqlIncrementalSource, "Use the actual CDC connector");
            JobContext jobContext = new JobContext(113L);
            jobContext.setJobMode(JobMode.STREAMING);
            jobContext.setEnableCheckpoint(true);
            source.setJobContext(jobContext);
            List<CatalogTable> sourceTables = source.getProducedCatalogTables();
            assertEquals(2, sourceTables.size());
            Map<TablePath, SeaTunnelSink> sinks = new LinkedHashMap<>();
            Map<String, FileStoreTable> targets = new LinkedHashMap<>();
            for (CatalogTable sourceTable : sourceTables) {
                String name = sourceTable.getTablePath().getTableName();
                Map<String, Object> sinkOptions = new HashMap<>();
                sinkOptions.put("warehouse", output.resolve("warehouse").toString());
                sinkOptions.put("plugin_name", "Paimon");
                sinkOptions.put("database", sourceTable.getTablePath().getDatabaseName());
                sinkOptions.put("table", name);
                Map<String, String> writeOptions = new HashMap<>();
                writeOptions.put("bucket", "16");
                writeOptions.put("write-only", "true");
                writeOptions.put("commit.timeout", "15 s");
                sinkOptions.put("paimon.table.write-props", writeOptions);
                ReadonlyConfig sinkConfig = ReadonlyConfig.fromMap(sinkOptions);
                PaimonCatalog catalog = new PaimonCatalog("paimon", sinkConfig);
                catalog.open();
                catalogs.add(catalog);
                TablePath targetPath = sourceTable.getTablePath();
                catalog.createDatabase(targetPath, true);
                catalog.createTable(targetPath, sourceTable, false);
                targets.put(name, (FileStoreTable) catalog.getPaimonTable(targetPath));
                PaimonSink tableSink =
                        fail
                                ? PaimonFlinkPendingRecoveryTest.failingSink(
                                        sinkConfig, sourceTable, runId)
                                : new PaimonSink(sinkConfig, sourceTable);
                tableSink.setJobContext(jobContext);
                sinks.put(sourceTable.getTablePath(), tableSink);
            }
            SeaTunnelSink sink =
                    new MultiTableSink(
                            new MultiTableFactoryContext(
                                    ReadonlyConfig.fromMap(Collections.emptyMap()),
                                    getClass().getClassLoader(),
                                    sinks));
            StreamExecutionEnvironment environment =
                    pipeline(
                            source,
                            sink,
                            sourceTables,
                            jobContext,
                            runId,
                            parallelism,
                            fail,
                            legacy,
                            null,
                            -1L);
            long submitted = System.nanoTime();
            client = environment.executeAsync("mysql-cdc-paimon-" + runId);
            awaitRows(client, targets, expected, progress, -1);
            long visible = System.nanoTime();
            evidence.setProperty(
                    "initialRowsVisibleMillis",
                    String.valueOf(TimeUnit.NANOSECONDS.toMillis(visible - submitted)));
            evidence.setProperty(
                    "firstRecordDelayMillis",
                    String.valueOf(
                            TimeUnit.NANOSECONDS.toMillis(
                                    progress.firstRecordNanos.get() - submitted)));
            evidence.setProperty(
                    "firstRecordToInitialVisibleMillis",
                    String.valueOf(
                            TimeUnit.NANOSECONDS.toMillis(
                                    visible - progress.firstRecordNanos.get())));
            evidence.setProperty("initialExpectedRecords", String.valueOf(2L * rows));
            evidence.setProperty(
                    "sourceRecordsAtInitialVisibility", String.valueOf(progress.records.get()));
            evidence.setProperty(
                    "initialRowsPerSecond",
                    String.valueOf(
                            2.0
                                    * rows
                                    * TimeUnit.SECONDS.toNanos(1)
                                    / (visible - progress.firstRecordNanos.get())));
            evidence.setProperty(
                    "restoredFlinkCheckpoint", String.valueOf(progress.restoredCheckpoint));
            if (fail) {
                assertTrue(PaimonFlinkPendingRecoveryTest.wasFailureInjected(runId));
                assertEquals(parallelism, PaimonFlinkPendingRecoveryTest.failureFragments(runId));
                assertEquals(
                        1, PaimonFlinkPendingRecoveryTest.failureCommitBoundaries(runId).size());
                assertTrue(
                        progress.restoredCheckpoint > 0, "CDC must restore a completed checkpoint");
            }
            int completedBeforeMutation = progress.completed.size();
            long mutationCommitted = 0;
            for (String name : tableNames) {
                mutate(jdbc, name, rows, 1);
                mutationCommitted = System.nanoTime();
                expected.put(name, readMySql(jdbc, name));
            }
            awaitRows(client, targets, expected, progress, -1);
            evidence.setProperty(
                    "mutationToVisibleMillis",
                    String.valueOf(
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - mutationCommitted)));
            awaitRows(client, targets, expected, progress, completedBeforeMutation + 3);
            evidence.setProperty(
                    "automaticRecoveryCheckpoint", String.valueOf(progress.restoredCheckpoint));
            evidence.setProperty(
                    "sourceRecordsBeforeSavepoint", String.valueOf(progress.records.get()));
            if (savepointEnabled) {
                evidence.setProperty("stoppedJobId", client.getJobID().toString());
                String savepoint =
                        client.stopWithSavepoint(
                                        false,
                                        output.resolve("savepoints").toUri().toString(),
                                        SavepointFormatType.CANONICAL)
                                .get(60, TimeUnit.SECONDS);
                client.getJobExecutionResult().get(60, TimeUnit.SECONDS);
                ObserverSavepoint savedObserver = inspectSavepoint(savepoint, parallelism);
                long savedCheckpoint = savedObserver.checkpointId;
                evidence.setProperty("savepointPath", savepoint);
                evidence.setProperty("savepointCheckpoint", String.valueOf(savedCheckpoint));
                evidence.setProperty(
                        "savepointFinishedObserverSubtasks",
                        String.valueOf(savedObserver.finishedSubtasks));
                evidence.setProperty(
                        "savepointExpectedObserverPartitions",
                        String.valueOf(savedObserver.statePartitions));
                writeEvidence(output, evidence);

                // Changes while the original job is stopped must be read from its saved binlog
                // offset.
                for (String name : tableNames) {
                    mutate(jdbc, name, rows, 2);
                    expected.put(name, readMySql(jdbc, name));
                }
                progress = new Progress();
                RUNS.put(runId, progress);
                SeaTunnelSource restoredSource =
                        new MySqlIncrementalSourceFactory()
                                .createSource(
                                        new TableSourceFactoryContext(
                                                ReadonlyConfig.fromMap(sourceOptions),
                                                getClass().getClassLoader()))
                                .createSource();
                restoredSource.setJobContext(jobContext);
                long restoreStarted = System.nanoTime();
                client =
                        pipeline(
                                        restoredSource,
                                        sink,
                                        sourceTables,
                                        jobContext,
                                        runId,
                                        parallelism,
                                        false,
                                        legacy,
                                        savepoint,
                                        savedCheckpoint)
                                .executeAsync("mysql-cdc-paimon-restored-" + runId);
                evidence.setProperty("restoredJobId", client.getJobID().toString());
                awaitRows(client, targets, expected, progress, 1);
                assertEquals(savedCheckpoint, progress.restoredCheckpoint);
                assertEquals(
                        savedObserver.statePartitions,
                        progress.restoredObserverPartitions.get(),
                        "Every observer partition saved by active subtasks must be restored");
                evidence.setProperty(
                        "savepointRestoredObserverPartitions",
                        String.valueOf(progress.restoredObserverPartitions.get()));
                evidence.setProperty(
                        "restoredFromSavepointCheckpoint",
                        String.valueOf(progress.restoredCheckpoint));
                evidence.setProperty(
                        "savepointCatchupMillis",
                        String.valueOf(
                                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - restoreStarted)));
                evidence.setProperty(
                        "savepointCatchupSourceRecords", String.valueOf(progress.records.get()));
                if (rows >= 1024) {
                    assertTrue(
                            progress.records.get() < rows,
                            "Savepoint catchup must not start another full table snapshot");
                }
                int restoredBeforeMutation = progress.completed.size();
                for (String name : tableNames) {
                    mutate(jdbc, name, rows, 3);
                    expected.put(name, readMySql(jdbc, name));
                }
                awaitRows(client, targets, expected, progress, restoredBeforeMutation + 3);
                assertTrue(progress.completedAfterRestore.size() >= 3);
                assertTrue(
                        progress.completed.stream().allMatch(id -> id > savedCheckpoint),
                        "Checkpoints after Savepoint restoration must advance past its exact ID");
            }
            evidence.setProperty("completedCheckpoints", String.valueOf(progress.completed.size()));
            evidence.setProperty(
                    "completedCheckpointsAfterRestore",
                    String.valueOf(progress.completedAfterRestore.size()));
            evidence.setProperty("sourceRecordsFinal", String.valueOf(progress.records.get()));
            for (Map.Entry<String, FileStoreTable> target : targets.entrySet()) {
                evidence.setProperty(
                        target.getKey() + ".snapshot",
                        String.valueOf(target.getValue().snapshotManager().latestSnapshotId()));
                evidence.setProperty(
                        target.getKey() + ".records",
                        String.valueOf(expected.get(target.getKey()).size()));
                evidence.setProperty(target.getKey() + ".completeKeysAndValues", "true");
            }
            evidence.setProperty("result", "PASS");
        } finally {
            if (client != null && !client.getJobExecutionResult().isDone()) {
                client.cancel().get(30, TimeUnit.SECONDS);
            }
            for (PaimonCatalog catalog : catalogs) {
                catalog.close();
            }
            evidence.putIfAbsent("result", "FAILED");
            evidence.setProperty(
                    "restoredFlinkCheckpoint", String.valueOf(progress.restoredCheckpoint));
            evidence.setProperty("sourceRecordsFinal", String.valueOf(progress.records.get()));
            writeEvidence(output, evidence);
            RUNS.remove(runId);
            if (fail) {
                PaimonFlinkPendingRecoveryTest.removeFailureControl(runId);
            }
            System.out.println(
                    "MySQL CDC acceptance evidence: " + output.resolve("run.properties"));
        }
    }

    private static StreamExecutionEnvironment pipeline(
            SeaTunnelSource source,
            SeaTunnelSink sink,
            List<CatalogTable> sourceTables,
            JobContext jobContext,
            String runId,
            int parallelism,
            boolean fail,
            boolean legacy,
            String savepoint,
            long expectedSavepoint) {
        Configuration flinkConfig = new Configuration();
        if (savepoint != null) {
            flinkConfig.setString("execution.savepoint.path", savepoint);
        }
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.createLocalEnvironment(parallelism, flinkConfig);
        environment.getConfig().disableClosureCleaner();
        environment.enableCheckpointing(
                positiveProperty("cf.mysql.acceptance.checkpointMillis", 1000));
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        environment.setRestartStrategy(
                fail ? RestartStrategies.fixedDelayRestart(1, 100) : RestartStrategies.noRestart());
        DataStream<SeaTunnelRow> input =
                environment
                        .fromSource(
                                new FlinkSource(source, ConfigFactory.empty()),
                                WatermarkStrategy.noWatermarks(),
                                "MySQL-CDC-Source")
                        .uid("acceptance-mysql-source")
                        .setParallelism(parallelism)
                        .map(new ObserveProgress(runId, expectedSavepoint))
                        .uid("acceptance-progress")
                        .setParallelism(parallelism);
        if (legacy) {
            PaimonFlinkPendingRecoveryTest.legacyRoutedSink(input, sink, sourceTables, parallelism)
                    .uid("acceptance-paimon-sink");
        } else {
            new SinkExecuteProcessor(
                            new ArrayList<>(),
                            ConfigFactory.empty(),
                            Collections.emptyList(),
                            jobContext)
                    .createVersionSpecificDataStreamSink(
                            new DataStreamTableInfo(input, sourceTables, "input"),
                            sink,
                            parallelism,
                            ConfigFactory.empty())
                    .uid("acceptance-paimon-sink");
        }
        return environment;
    }

    private static ObserverSavepoint inspectSavepoint(String savepoint, int parallelism)
            throws Exception {
        Path metadata = Paths.get(URI.create(savepoint)).resolve("_metadata");
        CheckpointMetadata checkpoint;
        try (DataInputStream input = new DataInputStream(Files.newInputStream(metadata))) {
            checkpoint =
                    Checkpoints.loadCheckpointMetadata(
                            input, PaimonMySqlCdcRecoveryIT.class.getClassLoader(), savepoint);
        }
        ObserverSavepoint observer = null;
        for (OperatorState operator : checkpoint.getOperatorStates()) {
            int partitions = 0;
            int finished = 0;
            for (OperatorSubtaskState subtask : operator.getStates()) {
                if (subtask.isFinished()) {
                    finished++;
                }
                for (OperatorStateHandle handle : subtask.getManagedOperatorState()) {
                    OperatorStateHandle.StateMetaInfo state =
                            handle.getStateNameToPartitionOffsets().get("observed-checkpoint");
                    if (state != null) {
                        partitions += state.getOffsets().length;
                    }
                }
            }
            if (partitions > 0) {
                assertTrue(
                        observer == null, "Savepoint must contain exactly one observer operator");
                assertEquals(parallelism, operator.getParallelism());
                assertEquals(parallelism, operator.getNumberCollectedStates());
                assertEquals(
                        parallelism - finished,
                        partitions,
                        "Each active observer must save one checkpoint marker");
                observer =
                        new ObserverSavepoint(checkpoint.getCheckpointId(), partitions, finished);
            }
        }
        assertTrue(observer != null, "Savepoint observer state is missing from metadata");
        return observer;
    }

    private static void writeCleanupSql(Path output, String database, List<String> tables)
            throws Exception {
        StringBuilder sql = new StringBuilder("-- Only synthetic tables created by this run.\n");
        for (String table : tables) {
            assertTrue(table.matches("cf_recovery_[0-9a-f]{16}_(narrow|wide)"));
            sql.append("DROP TABLE IF EXISTS `")
                    .append(database.replace("`", "``"))
                    .append("`.`")
                    .append(table)
                    .append("`;\n");
        }
        Files.write(output.resolve("cleanup.sql"), sql.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static Properties loadPrivateProperties() throws Exception {
        Path file = Paths.get(System.getProperty("cf.mysql.acceptance.config"));
        if (Files.getFileStore(file).supportsFileAttributeView("posix")) {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file);
            assertTrue(
                    permissions.stream()
                            .noneMatch(
                                    permission ->
                                            permission.name().startsWith("GROUP_")
                                                    || permission.name().startsWith("OTHERS_")),
                    "Connection properties must be readable only by their owner");
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(file)) {
            properties.load(input);
        }
        return properties;
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        assertTrue(value != null && !value.isEmpty(), "Missing connection property: " + key);
        return value;
    }

    private static int positiveProperty(String key, int fallback) {
        int value = Integer.getInteger(key, fallback);
        assertTrue(value > 0, key + " must be positive");
        return value;
    }

    private static void verifyBinlog(Connection jdbc) throws Exception {
        Map<String, String> values = new HashMap<>();
        try (Statement statement = jdbc.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SHOW VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','binlog_row_image')")) {
            while (result.next()) {
                values.put(result.getString(1), result.getString(2));
            }
        }
        assertEquals("ON", values.get("log_bin"));
        assertEquals("ROW", values.get("binlog_format"));
        assertEquals("FULL", values.get("binlog_row_image"));
    }

    private static void createAndFill(Connection jdbc, String table, int rows, int payloadSize)
            throws Exception {
        try (Statement statement = jdbc.createStatement()) {
            statement.execute(
                    "CREATE TABLE `"
                            + table
                            + "` (id BIGINT NOT NULL, version INT NOT NULL, value VARCHAR(128) NOT NULL, payload VARCHAR(4096), PRIMARY KEY(id)) ENGINE=InnoDB");
        }
        jdbc.setAutoCommit(false);
        String payload = String.join("", Collections.nCopies(payloadSize, "x"));
        try (PreparedStatement statement =
                jdbc.prepareStatement(
                        "INSERT INTO `"
                                + table
                                + "` (id,version,value,payload) VALUES (?,?,?,?)")) {
            for (long id = 0; id < rows; id++) {
                statement.setLong(1, id);
                statement.setInt(2, 0);
                statement.setString(3, "snapshot-" + id);
                statement.setString(4, id % 7 == 0 ? null : payload + id);
                statement.addBatch();
                if ((id + 1) % 500 == 0 || id + 1 == rows) {
                    statement.executeBatch();
                    jdbc.commit();
                }
            }
        } finally {
            jdbc.setAutoCommit(true);
        }
    }

    private static void mutate(Connection jdbc, String table, int rows, int phase)
            throws Exception {
        try (Statement statement = jdbc.createStatement()) {
            assertEquals(
                    1,
                    statement.executeUpdate(
                            "UPDATE `"
                                    + table
                                    + "` SET version="
                                    + phase
                                    + ",value='updated-phase-"
                                    + phase
                                    + "',payload="
                                    + (phase % 2 == 1 ? "NULL" : "'restored-payload'")
                                    + " WHERE id=0"));
            assertEquals(
                    1, statement.executeUpdate("DELETE FROM `" + table + "` WHERE id=" + phase));
            assertEquals(
                    1,
                    statement.executeUpdate(
                            "INSERT INTO `"
                                    + table
                                    + "` (id,version,value,payload) VALUES ("
                                    + (rows + phase - 1L)
                                    + ","
                                    + phase
                                    + ",'inserted-phase-"
                                    + phase
                                    + "','new-payload')"));
        }
    }

    private static Map<Long, List<Object>> readMySql(Connection jdbc, String table)
            throws Exception {
        Map<Long, List<Object>> rows = new LinkedHashMap<>();
        try (Statement statement = jdbc.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SELECT id,version,value,payload FROM `"
                                        + table
                                        + "` ORDER BY id")) {
            while (result.next()) {
                rows.put(
                        result.getLong(1),
                        Arrays.asList(result.getInt(2), result.getString(3), result.getString(4)));
            }
        }
        return rows;
    }

    private static Map<Long, List<Object>> readPaimon(FileStoreTable table) throws Exception {
        table.snapshotManager().invalidateCache();
        Map<Long, List<Object>> rows = new LinkedHashMap<>();
        ReadBuilder read = table.newReadBuilder();
        try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan());
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                List<Object> previous =
                        rows.put(
                                row.getLong(0),
                                Arrays.asList(
                                        row.getInt(1),
                                        row.getString(2).toString(),
                                        row.isNullAt(3) ? null : row.getString(3).toString()));
                assertTrue(
                        previous == null,
                        "Paimon must expose only one current row for each primary key");
            }
        }
        return rows;
    }

    private static void awaitRows(
            JobClient client,
            Map<String, FileStoreTable> targets,
            Map<String, Map<Long, List<Object>>> expected,
            Progress progress,
            int minimumCheckpoints)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(240);
        String observation = "no observation";
        while (System.nanoTime() < deadline) {
            if (client.getJobExecutionResult().isDone()) {
                client.getJobExecutionResult().get(5, TimeUnit.SECONDS);
            }
            boolean matches = true;
            StringBuilder summary = new StringBuilder();
            for (Map.Entry<String, FileStoreTable> target : targets.entrySet()) {
                Map<Long, List<Object>> actual = readPaimon(target.getValue());
                boolean equal = expected.get(target.getKey()).equals(actual);
                matches &= equal;
                summary.append(target.getKey())
                        .append(": expected=")
                        .append(expected.get(target.getKey()).size())
                        .append(", actual=")
                        .append(actual.size())
                        .append(", completeKeysAndValues=")
                        .append(equal)
                        .append("; ");
            }
            observation = summary.toString();
            if (matches && progress.completed.size() >= minimumCheckpoints) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError(
                "MySQL CDC did not converge: "
                        + observation
                        + "; restoredCheckpoint="
                        + progress.restoredCheckpoint
                        + "; completedCheckpoints="
                        + progress.completed.size());
    }

    private static void writeEvidence(Path output, Properties evidence) throws Exception {
        try (OutputStream file = Files.newOutputStream(output.resolve("run.properties"))) {
            evidence.store(file, "Synthetic MySQL CDC acceptance; no connection credentials");
        }
    }

    private static final class Progress {
        private final AtomicLong records = new AtomicLong();
        private final AtomicLong firstRecordNanos = new AtomicLong();
        private final Set<Long> completed = ConcurrentHashMap.newKeySet();
        private final Set<Long> completedAfterRestore = ConcurrentHashMap.newKeySet();
        private final AtomicLong restoredObserverPartitions = new AtomicLong();
        private volatile long restoredCheckpoint = -1;
    }

    private static final class ObserverSavepoint {
        private final long checkpointId;
        private final int statePartitions;
        private final int finishedSubtasks;

        private ObserverSavepoint(long checkpointId, int statePartitions, int finishedSubtasks) {
            this.checkpointId = checkpointId;
            this.statePartitions = statePartitions;
            this.finishedSubtasks = finishedSubtasks;
        }
    }

    private static final class ObserveProgress
            implements MapFunction<SeaTunnelRow, SeaTunnelRow>,
                    CheckpointedFunction,
                    CheckpointListener {
        private final String runId;
        private final long expectedSavepoint;
        private boolean restored;
        private transient ListState<Long> state;

        private ObserveProgress(String runId, long expectedSavepoint) {
            this.runId = runId;
            this.expectedSavepoint = expectedSavepoint;
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            Progress progress = RUNS.get(runId);
            progress.firstRecordNanos.compareAndSet(0, System.nanoTime());
            progress.records.incrementAndGet();
            return row;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.update(Collections.singletonList(context.getCheckpointId()));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>("observed-checkpoint", Long.class));
            restored = context.isRestored();
            if (expectedSavepoint >= 0) {
                if (!restored
                        || context.getRestoredCheckpointId().getAsLong() != expectedSavepoint) {
                    throw new IllegalStateException("The requested Savepoint was not restored");
                }
                int observed = 0;
                for (Long checkpoint : state.get()) {
                    if (checkpoint != expectedSavepoint) {
                        throw new IllegalStateException("Restored observer state has the wrong ID");
                    }
                    observed++;
                }
                // Finished snapshot readers legitimately have no split-distributed state. The
                // driver checks the total against active partitions in Savepoint metadata.
                RUNS.get(runId).restoredObserverPartitions.addAndGet(observed);
            }
            if (restored) {
                RUNS.get(runId).restoredCheckpoint = context.getRestoredCheckpointId().getAsLong();
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            Progress progress = RUNS.get(runId);
            progress.completed.add(checkpointId);
            if (restored) {
                progress.completedAfterRestore.add(checkpointId);
            }
        }
    }
}

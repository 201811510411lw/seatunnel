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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriteRouting;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.handler.PaimonSaveModeHandler;
import org.apache.seatunnel.connectors.seatunnel.paimon.security.PaimonSecurityContext;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket.PaimonBucketAssignerFactory;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.bucket.PaimonWriteRouting;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit.PaimonCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.state.PaimonSinkState;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.BranchManager;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Slf4j
public class PaimonSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        PaimonSinkState,
                        PaimonCommitInfo,
                        PaimonAggregatedCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink,
                SupportLoadTable<Table>,
                SupportSchemaEvolutionSink {

    private static final long serialVersionUID = 1L;

    public static final String PLUGIN_NAME = "Paimon";

    private FileStoreTable paimonTable;

    private JobContext jobContext;

    private final ReadonlyConfig readonlyConfig;

    private final PaimonSinkConfig paimonSinkConfig;

    private final CatalogTable catalogTable;

    private final PaimonHadoopConfiguration paimonHadoopConfiguration;

    private final PaimonBucketAssignerFactory paimonBucketAssignerFactory;

    private final String commitUser = UUID.randomUUID().toString();

    private SinkWriteRouting writeRouting;

    public PaimonSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.readonlyConfig = readonlyConfig;
        this.paimonSinkConfig = new PaimonSinkConfig(readonlyConfig);
        this.catalogTable = catalogTable;
        this.paimonHadoopConfiguration = PaimonSecurityContext.loadHadoopConfig(paimonSinkConfig);
        this.paimonBucketAssignerFactory = new PaimonBucketAssignerFactory();
        try (PaimonCatalog paimonCatalog = PaimonCatalog.loadPaimonCatalog(readonlyConfig)) {
            paimonCatalog.open();
            boolean databaseExists =
                    paimonCatalog.databaseExists(this.paimonSinkConfig.getNamespace());
            if (!databaseExists) {
                return;
            }
            TablePath tablePath = catalogTable.getTablePath();
            boolean tableExists = paimonCatalog.tableExists(tablePath);
            if (!tableExists) {
                return;
            }
            this.paimonTable = (FileStoreTable) paimonCatalog.getPaimonTable(tablePath);
            String branchName = paimonSinkConfig.getBranch();
            if (StringUtils.isNotEmpty(branchName)) {
                BranchManager branchManager = paimonTable.branchManager();
                if (!branchManager.branchExists(branchName)) {
                    throw new PaimonConnectorException(
                            PaimonConnectorErrorCode.BRANCH_NOT_EXISTS, branchName);
                }
                if (!branchManager.DEFAULT_MAIN_BRANCH.equalsIgnoreCase(branchName)) {
                    this.paimonTable = paimonTable.switchToBranch(branchName);
                    log.info("Switch to branch {}", branchName);
                }
            }
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public Optional<SinkWriteRouting> getWriteRouting() {
        if (paimonTable == null) {
            throw new IllegalStateException("Paimon table must be loaded before building routing");
        }
        writeRouting =
                new PaimonWriteRouting(
                        paimonTable, catalogTable.getTableSchema().toPhysicalRowDataType());
        return Optional.of(writeRouting);
    }

    @Override
    public PaimonSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        PaimonSinkWriter writer =
                new PaimonSinkWriter(
                        context,
                        readonlyConfig,
                        catalogTable,
                        paimonTable,
                        commitUser,
                        jobContext,
                        paimonSinkConfig,
                        paimonHadoopConfiguration,
                        paimonBucketAssignerFactory);
        writer.setWriteRouting(writeRouting);
        return writer;
    }

    @Override
    public Optional<SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.of(new PaimonAggregatedCommitter(paimonTable, paimonHadoopConfiguration));
    }

    @Override
    public SinkWriter<SeaTunnelRow, PaimonCommitInfo, PaimonSinkState> restoreWriter(
            SinkWriter.Context context, List<PaimonSinkState> states) throws IOException {
        if (writeRouting != null) {
            validateRoutedWriterState(context, states);
            if (context.getNumberOfParallelSubtasks() > 1) {
                PaimonSinkWriter writer = createWriter(context);
                writer.deferRecoveryUntilGlobalCommit(states.get(0));
                return writer;
            }
        }
        PaimonSinkWriter writer =
                new PaimonSinkWriter(
                        context,
                        readonlyConfig,
                        catalogTable,
                        paimonTable,
                        commitUser,
                        states,
                        jobContext,
                        paimonSinkConfig,
                        paimonHadoopConfiguration,
                        paimonBucketAssignerFactory);
        writer.setWriteRouting(writeRouting);
        return writer;
    }

    private void validateRoutedWriterState(
            SinkWriter.Context context, List<PaimonSinkState> states) {
        if (states == null || states.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot restore routed Paimon sink without writer state");
        }
        PaimonSinkState first = states.get(0);
        int previousParallelism = first.getWriterParallelism();
        int currentParallelism = context.getNumberOfParallelSubtasks();
        boolean rescaled = previousParallelism != currentParallelism;
        if (previousParallelism < 1
                || (first.getBucketRoutingVersion() != 1 && first.getBucketRoutingVersion() != 2)
                || (rescaled
                        && (currentParallelism != 1 || first.getBucketRoutingVersion() != 2))) {
            throw new IllegalStateException(
                    "Paimon writer restore requires compatible routed state; "
                            + "rescaling is supported only to one writer with routing state version 2");
        }
        int expectedStates = rescaled ? previousParallelism : 1;
        if (states.size() != expectedStates) {
            throw new IllegalStateException(
                    "Incomplete or duplicate Paimon writer state assignment");
        }
        Set<Integer> writerIndexes = new HashSet<>();
        for (PaimonSinkState state : states) {
            if (state.getBucketRoutingVersion() != first.getBucketRoutingVersion()
                    || state.getWriterParallelism() != previousParallelism
                    || state.getCheckpointId() != first.getCheckpointId()
                    || state.getCommitUser() == null
                    || !Objects.equals(state.getCommitUser(), first.getCommitUser())
                    || state.getCommitTables() == null) {
                throw new IllegalStateException("Inconsistent Paimon writer recovery boundary");
            }
            if (state.getBucketRoutingVersion() == 2
                    && (state.getWriterIndex() < 0
                            || state.getWriterIndex() >= previousParallelism
                            || !writerIndexes.add(state.getWriterIndex())
                            || (!rescaled
                                    && state.getWriterIndex() != context.getIndexOfSubtask()))) {
                throw new IllegalStateException("Invalid or duplicate Paimon writer identity");
            }
        }
    }

    @Override
    public Optional<Serializer<PaimonSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<PaimonAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<PaimonCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        PaimonCatalog paimonCatalog = PaimonCatalog.loadPaimonCatalog(readonlyConfig);
        return Optional.of(
                new PaimonSaveModeHandler(
                        this,
                        paimonSinkConfig.getSchemaSaveMode(),
                        paimonSinkConfig.getDataSaveMode(),
                        paimonCatalog,
                        catalogTable,
                        null,
                        paimonSinkConfig.getBranch()));
    }

    @Override
    public void setLoadTable(Table table) {
        this.paimonTable = (FileStoreTable) table;
    }

    @Override
    public Table getLoadTable() {
        return paimonTable;
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }

    @Override
    public List<SchemaChangeType> supports() {
        return Arrays.asList(
                SchemaChangeType.ADD_COLUMN,
                SchemaChangeType.DROP_COLUMN,
                SchemaChangeType.RENAME_COLUMN,
                SchemaChangeType.UPDATE_COLUMN);
    }
}

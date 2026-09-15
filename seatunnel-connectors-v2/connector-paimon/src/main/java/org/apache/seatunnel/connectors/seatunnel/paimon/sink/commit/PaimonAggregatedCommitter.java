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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.commit;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.security.PaimonSecurityContext;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Paimon connector aggregated committer class */
@Slf4j
public class PaimonAggregatedCommitter
        implements SinkAggregatedCommitter<PaimonCommitInfo, PaimonAggregatedCommitInfo>,
                SupportMultiTableSinkAggregatedCommitter {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;

    private final boolean confirmEmptyRecovery;

    public PaimonAggregatedCommitter(
            Table table, PaimonHadoopConfiguration paimonHadoopConfiguration) {
        this(table, paimonHadoopConfiguration, false);
    }

    /**
     * Enables empty-boundary confirmation only for an engine that activated fixed-bucket routing.
     * Ordinary committers retain their configured empty-commit behavior, including during recovery.
     */
    public PaimonAggregatedCommitter(
            Table table,
            PaimonHadoopConfiguration paimonHadoopConfiguration,
            boolean confirmEmptyRecovery) {
        this.table = (FileStoreTable) table;
        this.confirmEmptyRecovery = confirmEmptyRecovery;
        PaimonSecurityContext.shouldEnableKerberos(paimonHadoopConfiguration);
    }

    @Override
    public List<PaimonAggregatedCommitInfo> commit(
            List<PaimonAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        return commit(aggregatedCommitInfo, false);
    }

    /** Publishes empty restored boundaries when routed writers must verify their recovery. */
    @Override
    public List<PaimonAggregatedCommitInfo> restoreCommit(
            List<PaimonAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        return commit(aggregatedCommitInfo, true);
    }

    private List<PaimonAggregatedCommitInfo> commit(
            List<PaimonAggregatedCommitInfo> aggregatedCommitInfo, boolean restoring) {
        aggregatedCommitInfo.stream()
                .collect(Collectors.groupingBy(PaimonAggregatedCommitInfo::getCommitUser))
                .forEach((commitUser, commits) -> commit(commitUser, commits, restoring));
        return Collections.emptyList();
    }

    /** Commits a complete user/checkpoint group, publishing empty boundaries only on recovery. */
    private void commit(
            String commitUser,
            List<PaimonAggregatedCommitInfo> aggregatedCommitInfo,
            boolean restoring) {
        try (TableCommitImpl tableCommit = table.newCommit(commitUser)) {
            if (restoring && confirmEmptyRecovery) {
                // Restored writers also wait for a completed boundary containing no data files.
                // This override belongs to this invocation's TableCommitImpl, which is closed
                // below. Neither table options nor subsequent normal commits are changed.
                tableCommit.ignoreEmptyCommit(false);
            }
            PaimonSecurityContext.runSecured(
                    () -> {
                        log.debug("Trying to commit states streaming mode");
                        Map<Long, List<CommitMessage>> committablesMap =
                                mergeCommittables(aggregatedCommitInfo);
                        if (!committablesMap.isEmpty()) {
                            tableCommit.filterAndCommit(committablesMap);
                        }
                        return null;
                    });
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_WRITE_COMMIT_FAILED, e);
        }
    }

    /**
     * Combines all writer fragments for each checkpoint before invoking Paimon's idempotent commit.
     */
    @Override
    public PaimonAggregatedCommitInfo combine(List<PaimonCommitInfo> commitInfos) {
        String commitUser = commitInfos.get(0).getCommitUser();
        Map<Long, List<CommitMessage>> commitTables = new HashMap<>();
        commitInfos.forEach(
                commitInfo ->
                        commitTables
                                .computeIfAbsent(
                                        commitInfo.getCheckpointId(), id -> new ArrayList<>())
                                .addAll(commitInfo.getCommittables()));
        return new PaimonAggregatedCommitInfo(commitTables, commitUser);
    }

    @Override
    public void abort(List<PaimonAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {
        aggregatedCommitInfo.stream()
                .collect(Collectors.groupingBy(PaimonAggregatedCommitInfo::getCommitUser))
                .forEach(this::abort);
    }

    private void abort(String commitUser, List<PaimonAggregatedCommitInfo> aggregatedCommitInfo) {
        try (TableCommitImpl tableCommit = table.newCommit(commitUser)) {
            PaimonSecurityContext.runSecured(
                    () -> {
                        log.debug("Trying to commit states streaming mode");
                        Map<Long, List<CommitMessage>> committablesMap =
                                mergeCommittables(aggregatedCommitInfo);
                        if (!committablesMap.isEmpty()) {
                            committablesMap.values().forEach(tableCommit::abort);
                        }
                        return null;
                    });
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.TABLE_WRITE_COMMIT_FAILED, e);
        }
    }

    /** Merges every writer fragment sharing a checkpoint without dropping duplicate map keys. */
    private Map<Long, List<CommitMessage>> mergeCommittables(
            List<PaimonAggregatedCommitInfo> aggregatedCommitInfo) {
        Map<Long, List<CommitMessage>> committablesMap = new HashMap<>();
        aggregatedCommitInfo.forEach(
                info ->
                        info.getCommittablesMap()
                                .forEach(
                                        (checkpointId, committables) ->
                                                committablesMap
                                                        .computeIfAbsent(
                                                                checkpointId,
                                                                id -> new ArrayList<>())
                                                        .addAll(committables)));
        return committablesMap;
    }

    @Override
    public void close() throws IOException {}
}

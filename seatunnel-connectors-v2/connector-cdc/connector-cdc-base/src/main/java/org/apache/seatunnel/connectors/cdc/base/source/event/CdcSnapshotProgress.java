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

package org.apache.seatunnel.connectors.cdc.base.source.event;

import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import io.debezium.relational.TableId;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CdcSnapshotProgress implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int TABLE_DETAIL_LIMIT = 200;
    private static final int SPLIT_DETAIL_LIMIT = 500;

    private final boolean splitPlanningComplete;
    private final int totalTables;
    private final int completedTables;
    private final int processingTables;
    private final int queuedTables;
    private final int waitingForSplitPlanningTables;
    private final int unknownTables;
    private final int totalSplits;
    private final int completedSplits;
    private final int assignedSplits;
    private final int waitingSplits;
    private final int unknownSplits;
    private final boolean splitProgressAvailable;
    private final boolean tablesTruncated;
    private final boolean splitsTruncated;
    private final List<CdcTableProgress> tables;

    private CdcSnapshotProgress(
            boolean splitPlanningComplete,
            int totalTables,
            int completedTables,
            int processingTables,
            int queuedTables,
            int waitingForSplitPlanningTables,
            int unknownTables,
            int totalSplits,
            int completedSplits,
            int assignedSplits,
            int waitingSplits,
            int unknownSplits,
            boolean splitProgressAvailable,
            boolean tablesTruncated,
            boolean splitsTruncated,
            List<CdcTableProgress> tables) {
        this.splitPlanningComplete = splitPlanningComplete;
        this.totalTables = totalTables;
        this.completedTables = completedTables;
        this.processingTables = processingTables;
        this.queuedTables = queuedTables;
        this.waitingForSplitPlanningTables = waitingForSplitPlanningTables;
        this.unknownTables = unknownTables;
        this.totalSplits = totalSplits;
        this.completedSplits = completedSplits;
        this.assignedSplits = assignedSplits;
        this.waitingSplits = waitingSplits;
        this.unknownSplits = unknownSplits;
        this.splitProgressAvailable = splitProgressAvailable;
        this.tablesTruncated = tablesTruncated;
        this.splitsTruncated = splitsTruncated;
        this.tables = tables;
    }

    public static CdcSnapshotProgress from(
            Collection<TableId> capturedTables,
            Collection<TableId> remainingTables,
            Collection<TableId> alreadyProcessedTables,
            Collection<SnapshotSplit> remainingSplits,
            Map<String, SnapshotSplit> assignedSplits,
            Map<String, SnapshotSplitWatermark> completedOffsets) {
        Set<TableId> allTables = new HashSet<>();
        addAll(allTables, capturedTables);
        addAll(allTables, remainingTables);
        addAll(allTables, alreadyProcessedTables);
        Map<String, SplitState> splitStates = new HashMap<>();
        addSplits(allTables, splitStates, remainingSplits, completedOffsets, false);
        addSplits(
                allTables,
                splitStates,
                assignedSplits == null ? null : assignedSplits.values(),
                completedOffsets,
                true);
        Map<TableId, List<SplitState>> splitsByTable = new HashMap<>();
        int completedSplitCount = 0;
        int assignedSplitCount = 0;
        int waitingSplitCount = 0;
        int unknownSplitCount = 0;
        for (SplitState split : splitStates.values()) {
            splitsByTable.computeIfAbsent(split.tableId, ignored -> new ArrayList<>()).add(split);
            switch (split.status) {
                case COMPLETED:
                    completedSplitCount++;
                    break;
                case ASSIGNED:
                    assignedSplitCount++;
                    break;
                case QUEUED:
                    waitingSplitCount++;
                    break;
                default:
                    unknownSplitCount++;
            }
        }

        Set<TableId> unplanned = copySet(remainingTables);
        Set<TableId> planned = copySet(alreadyProcessedTables);
        boolean splitProgressAvailable = splitsByTable.keySet().containsAll(planned);
        List<TableId> ordered = new ArrayList<>(allTables);
        Collections.sort(ordered, Comparator.comparing(TableId::toString));
        List<CdcTableProgress> details = new ArrayList<>();
        int completed = 0;
        int processing = 0;
        int queued = 0;
        int waitingForPlanning = 0;
        int unknown = 0;
        int emittedSplits = 0;

        for (TableId tableId : ordered) {
            List<SplitState> tableSplits =
                    splitsByTable.getOrDefault(tableId, Collections.emptyList());
            Collections.sort(tableSplits, Comparator.comparing(split -> split.id));
            SplitCounts tableCounts = SplitCounts.from(tableSplits);
            CdcTableProgressStatus status;
            if (unplanned.contains(tableId)) {
                status = CdcTableProgressStatus.WAITING_FOR_SPLIT_PLANNING;
                waitingForPlanning++;
            } else if (tableCounts.processing > 0) {
                status = CdcTableProgressStatus.PROCESSING;
                processing++;
            } else if (tableCounts.queued > 0) {
                status = CdcTableProgressStatus.QUEUED;
                queued++;
            } else if (tableCounts.unknown > 0) {
                status = CdcTableProgressStatus.UNKNOWN;
                unknown++;
            } else if (planned.contains(tableId)) {
                status = CdcTableProgressStatus.COMPLETED;
                completed++;
            } else {
                status = CdcTableProgressStatus.UNKNOWN;
                unknown++;
            }
            if (details.size() < TABLE_DETAIL_LIMIT) {
                List<CdcSplitProgress> splitDetails = new ArrayList<>();
                for (SplitState split : tableSplits) {
                    if (emittedSplits >= SPLIT_DETAIL_LIMIT) {
                        break;
                    }
                    splitDetails.add(new CdcSplitProgress(split.id, split.status));
                    emittedSplits++;
                }
                Integer totalSplits =
                        unplanned.contains(tableId) || tableCounts.total == 0
                                ? null
                                : tableCounts.total;
                details.add(
                        new CdcTableProgress(
                                tableId.toString(),
                                status,
                                totalSplits,
                                totalSplits == null ? null : tableCounts.completed,
                                totalSplits == null ? null : tableCounts.processing,
                                totalSplits == null ? null : tableCounts.queued,
                                totalSplits == null ? null : tableCounts.unknown,
                                splitDetails));
            }
        }
        return new CdcSnapshotProgress(
                unplanned.isEmpty(),
                ordered.size(),
                completed,
                processing,
                queued,
                waitingForPlanning,
                unknown,
                splitStates.size(),
                completedSplitCount,
                assignedSplitCount,
                waitingSplitCount,
                unknownSplitCount,
                splitProgressAvailable,
                ordered.size() > TABLE_DETAIL_LIMIT,
                splitStates.size() > emittedSplits,
                details);
    }

    private static void addSplits(
            Set<TableId> allTables,
            Map<String, SplitState> splitStates,
            Collection<SnapshotSplit> splits,
            Map<String, SnapshotSplitWatermark> completedOffsets,
            boolean assigned) {
        if (splits == null) {
            return;
        }
        for (SnapshotSplit split : splits) {
            TableId tableId = split.getTableId();
            allTables.add(tableId);
            CdcSplitProgressStatus status =
                    !assigned
                            ? CdcSplitProgressStatus.QUEUED
                            : completedOffsets != null
                                            && completedOffsets.containsKey(split.splitId())
                                    ? CdcSplitProgressStatus.COMPLETED
                                    : CdcSplitProgressStatus.ASSIGNED;
            SplitState existing = splitStates.get(split.splitId());
            if (existing == null) {
                splitStates.put(split.splitId(), new SplitState(split.splitId(), tableId, status));
            } else if (!existing.tableId.equals(tableId) || existing.status != status) {
                existing.status = CdcSplitProgressStatus.UNKNOWN;
            }
        }
    }

    private static void addAll(Set<TableId> target, Collection<TableId> values) {
        if (values != null) {
            target.addAll(values);
        }
    }

    private static Set<TableId> copySet(Collection<TableId> values) {
        return values == null ? Collections.emptySet() : new HashSet<>(values);
    }

    public boolean isSplitPlanningComplete() {
        return splitPlanningComplete;
    }

    public int getTotalTables() {
        return totalTables;
    }

    public int getCompletedTables() {
        return completedTables;
    }

    public int getProcessingTables() {
        return processingTables;
    }

    public int getQueuedTables() {
        return queuedTables;
    }

    public int getWaitingForSplitPlanningTables() {
        return waitingForSplitPlanningTables;
    }

    public int getWaitingTables() {
        return queuedTables + waitingForSplitPlanningTables;
    }

    public int getUnknownTables() {
        return unknownTables;
    }

    public int getTotalSplits() {
        return totalSplits;
    }

    public int getCompletedSplits() {
        return completedSplits;
    }

    public int getAssignedSplits() {
        return assignedSplits;
    }

    public int getWaitingSplits() {
        return waitingSplits;
    }

    public int getUnknownSplits() {
        return unknownSplits;
    }

    public boolean isSplitProgressAvailable() {
        return splitProgressAvailable;
    }

    public boolean isTablesTruncated() {
        return tablesTruncated;
    }

    public boolean isSplitsTruncated() {
        return splitsTruncated;
    }

    public List<CdcTableProgress> getTables() {
        return tables;
    }

    CdcTableProgress table(String tableName) {
        return tables.stream()
                .filter(table -> table.getTable().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown table " + tableName));
    }

    void appendJson(StringBuilder json) {
        json.append(",\"splitPlanningComplete\":").append(splitPlanningComplete);
        json.append(",\"summary\":{")
                .append("\"totalTables\":")
                .append(totalTables)
                .append(",\"completedTables\":")
                .append(completedTables)
                .append(",\"processingTables\":")
                .append(processingTables)
                .append(",\"waitingTables\":")
                .append(getWaitingTables())
                .append(",\"queuedTables\":")
                .append(queuedTables)
                .append(",\"waitingForSplitPlanningTables\":")
                .append(waitingForSplitPlanningTables)
                .append(",\"unknownTables\":")
                .append(unknownTables)
                .append('}');
        json.append(",\"splitProgressAvailable\":").append(splitProgressAvailable);
        if (splitProgressAvailable) {
            json.append(",\"splitSummary\":{")
                    .append("\"totalSplits\":")
                    .append(totalSplits)
                    .append(",\"completedSplits\":")
                    .append(completedSplits)
                    .append(",\"assignedSplits\":")
                    .append(assignedSplits)
                    .append(",\"waitingSplits\":")
                    .append(waitingSplits)
                    .append(",\"unknownSplits\":")
                    .append(unknownSplits)
                    .append('}');
        }
        json.append(",\"tablesTruncated\":")
                .append(tablesTruncated)
                .append(",\"splitsTruncated\":")
                .append(splitsTruncated)
                .append(",\"tables\":[");
        for (int index = 0; index < tables.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            CdcTableProgress table = tables.get(index);
            json.append("{\"table\":\"")
                    .append(escape(table.getTable()))
                    .append("\",\"status\":\"")
                    .append(table.getStatus().name())
                    .append('\"');
            if (table.getTotalSplits() != null) {
                json.append(",\"totalSplits\":")
                        .append(table.getTotalSplits())
                        .append(",\"completedSplits\":")
                        .append(table.getCompletedSplits())
                        .append(",\"processingSplits\":")
                        .append(table.getProcessingSplits())
                        .append(",\"queuedSplits\":")
                        .append(table.getQueuedSplits())
                        .append(",\"unknownSplits\":")
                        .append(table.getUnknownSplits())
                        .append(",\"splits\":[");
                for (int splitIndex = 0; splitIndex < table.getSplits().size(); splitIndex++) {
                    if (splitIndex > 0) {
                        json.append(',');
                    }
                    CdcSplitProgress split = table.getSplits().get(splitIndex);
                    json.append("{\"id\":\"")
                            .append(escape(split.getId()))
                            .append("\",\"status\":\"")
                            .append(split.getStatus().name())
                            .append("\"}");
                }
                json.append(']');
            }
            json.append('}');
        }
        json.append(']');
    }

    private static String escape(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '\"':
                    escaped.append("\\\"");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    if (character < 0x20) {
                        escaped.append("\\u00");
                        escaped.append(Character.forDigit((character >> 4) & 0xF, 16));
                        escaped.append(Character.forDigit(character & 0xF, 16));
                    } else {
                        escaped.append(character);
                    }
            }
        }
        return escaped.toString();
    }

    private static class SplitCounts {
        private int total;
        private int completed;
        private int processing;
        private int queued;
        private int unknown;

        private static SplitCounts from(Collection<SplitState> splits) {
            SplitCounts counts = new SplitCounts();
            for (SplitState split : splits) {
                counts.total++;
                switch (split.status) {
                    case COMPLETED:
                        counts.completed++;
                        break;
                    case ASSIGNED:
                        counts.processing++;
                        break;
                    case QUEUED:
                        counts.queued++;
                        break;
                    default:
                        counts.unknown++;
                }
            }
            return counts;
        }
    }

    private static class SplitState {
        private final String id;
        private final TableId tableId;
        private CdcSplitProgressStatus status;

        private SplitState(String id, TableId tableId, CdcSplitProgressStatus status) {
            this.id = id;
            this.tableId = tableId;
            this.status = status;
        }
    }
}

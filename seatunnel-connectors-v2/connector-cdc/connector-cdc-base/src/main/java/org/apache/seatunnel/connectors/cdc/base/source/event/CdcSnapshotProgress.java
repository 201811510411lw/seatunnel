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

    private final boolean splitPlanningComplete;
    private final int totalTables;
    private final int completedTables;
    private final int processingTables;
    private final int queuedTables;
    private final int waitingForSplitPlanningTables;
    private final int unknownTables;
    private final boolean tablesTruncated;
    private final List<CdcTableProgress> tables;

    private CdcSnapshotProgress(
            boolean splitPlanningComplete,
            int totalTables,
            int completedTables,
            int processingTables,
            int queuedTables,
            int waitingForSplitPlanningTables,
            int unknownTables,
            boolean tablesTruncated,
            List<CdcTableProgress> tables) {
        this.splitPlanningComplete = splitPlanningComplete;
        this.totalTables = totalTables;
        this.completedTables = completedTables;
        this.processingTables = processingTables;
        this.queuedTables = queuedTables;
        this.waitingForSplitPlanningTables = waitingForSplitPlanningTables;
        this.unknownTables = unknownTables;
        this.tablesTruncated = tablesTruncated;
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
        Map<TableId, SplitCounts> counts = new HashMap<>();
        addSplits(allTables, counts, remainingSplits, completedOffsets, false);
        addSplits(allTables, counts, assignedSplits.values(), completedOffsets, true);

        Set<TableId> unplanned = copySet(remainingTables);
        Set<TableId> planned = copySet(alreadyProcessedTables);
        List<TableId> ordered = new ArrayList<>(allTables);
        Collections.sort(ordered, Comparator.comparing(TableId::toString));
        List<CdcTableProgress> details = new ArrayList<>();
        int completed = 0;
        int processing = 0;
        int queued = 0;
        int waitingForPlanning = 0;
        int unknown = 0;

        for (TableId tableId : ordered) {
            SplitCounts tableCounts = counts.getOrDefault(tableId, new SplitCounts());
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
            } else if (planned.contains(tableId)) {
                status = CdcTableProgressStatus.COMPLETED;
                completed++;
            } else {
                status = CdcTableProgressStatus.UNKNOWN;
                unknown++;
            }
            if (details.size() < TABLE_DETAIL_LIMIT) {
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
                                totalSplits == null ? null : tableCounts.queued));
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
                ordered.size() > TABLE_DETAIL_LIMIT,
                details);
    }

    private static void addSplits(
            Set<TableId> allTables,
            Map<TableId, SplitCounts> counts,
            Collection<SnapshotSplit> splits,
            Map<String, SnapshotSplitWatermark> completedOffsets,
            boolean assigned) {
        if (splits == null) {
            return;
        }
        for (SnapshotSplit split : splits) {
            TableId tableId = split.getTableId();
            allTables.add(tableId);
            SplitCounts tableCounts = counts.computeIfAbsent(tableId, ignored -> new SplitCounts());
            tableCounts.total++;
            if (!assigned) {
                tableCounts.queued++;
            } else if (completedOffsets != null && completedOffsets.containsKey(split.splitId())) {
                tableCounts.completed++;
            } else {
                tableCounts.processing++;
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

    public boolean isTablesTruncated() {
        return tablesTruncated;
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
        json.append(",\"tablesTruncated\":").append(tablesTruncated).append(",\"tables\":[");
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
                        .append(table.getQueuedSplits());
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
    }
}

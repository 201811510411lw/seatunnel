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

import java.io.Serializable;
import java.util.List;

public class CdcTableProgress implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String table;
    private final CdcTableProgressStatus status;
    private final Integer totalSplits;
    private final Integer completedSplits;
    private final Integer processingSplits;
    private final Integer queuedSplits;
    private final Integer unknownSplits;
    private final List<CdcSplitProgress> splits;

    CdcTableProgress(
            String table,
            CdcTableProgressStatus status,
            Integer totalSplits,
            Integer completedSplits,
            Integer processingSplits,
            Integer queuedSplits,
            Integer unknownSplits,
            List<CdcSplitProgress> splits) {
        this.table = table;
        this.status = status;
        this.totalSplits = totalSplits;
        this.completedSplits = completedSplits;
        this.processingSplits = processingSplits;
        this.queuedSplits = queuedSplits;
        this.unknownSplits = unknownSplits;
        this.splits = splits;
    }

    public String getTable() {
        return table;
    }

    public CdcTableProgressStatus getStatus() {
        return status;
    }

    public Integer getTotalSplits() {
        return totalSplits;
    }

    public Integer getCompletedSplits() {
        return completedSplits;
    }

    public Integer getProcessingSplits() {
        return processingSplits;
    }

    public Integer getQueuedSplits() {
        return queuedSplits;
    }

    public Integer getUnknownSplits() {
        return unknownSplits;
    }

    public List<CdcSplitProgress> getSplits() {
        return splits;
    }

    CdcSplitProgress split(String splitId) {
        return splits.stream()
                .filter(split -> split.getId().equals(splitId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown split " + splitId));
    }
}

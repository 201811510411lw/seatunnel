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

package org.apache.seatunnel.api.sink;

/**
 * Optional sink capability for recovery protocols that require complete writer and global commit
 * state.
 *
 * <p>When this capability is required, engines must restore the global commit path and finish
 * pending global commits before writers resume writing. This protocol is independent from write
 * routing: a sink may require recovery without custom routing, or route records without requiring
 * global commit recovery.
 */
public interface SupportSinkGlobalCommitRecovery {

    /**
     * Returns whether writers must wait for complete global commit recovery before resuming writes.
     * Called after engine routing setup; implementations must not rebuild routing on this query.
     */
    boolean requiresGlobalCommitRecovery();

    /**
     * Resolves the recovery requirement during setup or lifecycle handling, not for each record.
     */
    static boolean isRequired(SeaTunnelSink<?, ?, ?, ?> sink) {
        return sink instanceof SupportSinkGlobalCommitRecovery
                && ((SupportSinkGlobalCommitRecovery) sink).requiresGlobalCommitRecovery();
    }
}

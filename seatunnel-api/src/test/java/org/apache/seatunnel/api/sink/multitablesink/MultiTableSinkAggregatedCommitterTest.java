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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MultiTableSinkAggregatedCommitterTest {

    @Test
    @SuppressWarnings("unchecked")
    void shouldForwardRecoveryToEachTableAndPreserveRetries() throws IOException {
        SinkAggregatedCommitter<String, String> first = mock(SinkAggregatedCommitter.class);
        SinkAggregatedCommitter<String, String> second = mock(SinkAggregatedCommitter.class);
        List<String> firstInput = Collections.singletonList("first-state");
        List<String> secondInput = Arrays.asList("second-state-1", "second-state-2");
        when(first.restoreCommit(firstInput)).thenReturn(Collections.singletonList("first-retry"));
        when(second.restoreCommit(secondInput))
                .thenReturn(Arrays.asList("second-retry-1", "second-retry-2"));
        when(first.commit(firstInput)).thenReturn(Collections.emptyList());
        when(second.commit(secondInput)).thenReturn(Collections.emptyList());
        Map<String, SinkAggregatedCommitter<?, ?>> tableCommitters = new HashMap<>();
        tableCommitters.put("first", first);
        tableCommitters.put("second", second);
        MultiTableSinkAggregatedCommitter committer =
                new MultiTableSinkAggregatedCommitter(tableCommitters);
        Map<String, Object> firstBoundary = new HashMap<>();
        firstBoundary.put("first", "first-state");
        firstBoundary.put("second", "second-state-1");
        List<MultiTableAggregatedCommitInfo> input =
                Arrays.asList(
                        new MultiTableAggregatedCommitInfo(firstBoundary),
                        new MultiTableAggregatedCommitInfo(
                                Collections.singletonMap("second", "second-state-2")));

        List<MultiTableAggregatedCommitInfo> retries = committer.restoreCommit(input);
        Assertions.assertEquals(2, retries.size());
        Map<String, Object> firstRetry = new HashMap<>();
        firstRetry.put("first", "first-retry");
        firstRetry.put("second", "second-retry-1");
        Assertions.assertEquals(firstRetry, retries.get(0).getCommitInfo());
        Assertions.assertEquals(
                Collections.singletonMap("second", "second-retry-2"),
                retries.get(1).getCommitInfo());
        verify(first).restoreCommit(firstInput);
        verify(second).restoreCommit(secondInput);
        verify(first, never()).commit(anyList());
        verify(second, never()).commit(anyList());

        Assertions.assertTrue(committer.commit(input).isEmpty());
        verify(first).commit(firstInput);
        verify(second).commit(secondInput);
        verifyNoMoreInteractions(first, second);
    }

    @Test
    void testInitBeInvoked() throws IOException {
        Map<String, SinkAggregatedCommitter<?, ?>> aggCommitters = new HashMap<>();
        List<String> methodInvoked = new ArrayList<>();
        aggCommitters.put(
                "table1",
                new SinkAggregatedCommitter<Object, Object>() {

                    @Override
                    public void init() {
                        methodInvoked.add("init");
                    }

                    @Override
                    public List<Object> commit(List<Object> aggregatedCommitInfo)
                            throws IOException {
                        return Collections.emptyList();
                    }

                    @Override
                    public Object combine(List<Object> commitInfos) {
                        return null;
                    }

                    @Override
                    public void abort(List<Object> aggregatedCommitInfo) throws Exception {}

                    @Override
                    public void close() throws IOException {
                        methodInvoked.add("close");
                    }
                });
        MultiTableSinkAggregatedCommitter committer =
                new MultiTableSinkAggregatedCommitter(aggCommitters);
        committer.init();
        committer.close();
        Assertions.assertIterableEquals(Arrays.asList("init", "close"), methodInvoked);
    }
}

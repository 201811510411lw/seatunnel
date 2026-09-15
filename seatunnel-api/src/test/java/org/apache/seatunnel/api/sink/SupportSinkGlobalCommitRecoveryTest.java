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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SupportSinkGlobalCommitRecoveryTest {

    @Test
    void shouldResolveOnlyRecoveryCapability() {
        assertFalse(SupportSinkGlobalCommitRecovery.isRequired(new PlainSink()));
        assertFalse(SupportSinkGlobalCommitRecovery.isRequired(new RecoverySink(false)));
        assertTrue(SupportSinkGlobalCommitRecovery.isRequired(new RecoverySink(true)));
    }

    private static class PlainSink implements SeaTunnelSink<String, Void, Void, Void> {

        @Override
        public String getPluginName() {
            return "test-sink";
        }

        @Override
        public SinkWriter<String, Void, Void> createWriter(SinkWriter.Context context) {
            throw new UnsupportedOperationException("This fixture only provides sink capability");
        }
    }

    private static class RecoverySink extends PlainSink implements SupportSinkGlobalCommitRecovery {

        private final boolean required;

        private RecoverySink(boolean required) {
            this.required = required;
        }

        @Override
        public boolean requiresGlobalCommitRecovery() {
            return required;
        }
    }
}

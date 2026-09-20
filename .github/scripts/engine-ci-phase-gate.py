# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Throwaway diagnostic gates; never apply to the production repair branch."""
from pathlib import Path


def replace_once(text, before, after):
    assert text.count(before) == 1, before
    return text.replace(before, after, 1)


coordinator = Path('seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/checkpoint/CheckpointCoordinator.java')
test = Path('seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/java/org/apache/seatunnel/engine/e2e/CheckpointCoordinatorFailoverIT.java')
source = coordinator.read_text()
helper = '''    /** Diagnostic-only gate: control the phase of the original fault injection. */
    private void awaitDiagnosticFaultInjection(long checkpointId, String phase) {
        if (checkpointId != 2L
                || !phase.equals(System.getProperty("seatunnel.diagnostic.fault-phase"))) {
            return;
        }
        System.out.println("[DEBUG-engine-ci] ready phase=" + phase + " checkpoint=" + checkpointId);
        System.setProperty("seatunnel.diagnostic.ready", phase);
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        while (!Boolean.getBoolean("seatunnel.diagnostic.release")) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Diagnostic fault injection was not released");
            }
            java.util.concurrent.locks.LockSupport.parkNanos(
                    java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(10));
        }
        System.out.println("[DEBUG-engine-ci] released phase=" + phase);
    }

'''
marker = '    @VisibleForTesting\n    protected boolean notifyCompleted(CompletedCheckpoint completedCheckpoint) {'
source = replace_once(source, marker, helper + marker)
marker = '                InvocationFuture<?>[] invocationFutures =\n                        notifyCheckpointCompleted(completedCheckpoint);'
source = replace_once(source, marker,
    '                awaitDiagnosticFaultInjection(completedCheckpoint.getCheckpointId(), "notify");\n' + marker)
marker = '                    // Trigger the barrier and wait for all tasks to ACK\n'
source = replace_once(source, marker, marker +
    '                    awaitDiagnosticFaultInjection(pendingCheckpoint.getCheckpointId(), "trigger");\n')
coordinator.write_text(source)

source = test.read_text()
marker = '                                                + " before injecting the fault");\n'
source = replace_once(source, marker, marker + '''                                Assertions.assertEquals(
                                        System.getProperty("seatunnel.diagnostic.fault-phase"),
                                        System.getProperty("seatunnel.diagnostic.ready"),
                                        "Waiting for the selected checkpoint phase");
''')
marker = '                    "the running task\'s slot-profile bookkeeping should exist before injection");\n'
source = replace_once(source, marker, marker + '''            System.out.println("[DEBUG-engine-ci] task addresses removed; releasing selected phase");
            System.setProperty("seatunnel.diagnostic.release", "true");
''')
test.write_text(source)
print('Applied diagnostic phase gates; original error assertion is unchanged.')

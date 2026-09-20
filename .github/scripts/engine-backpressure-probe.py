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

"""Diagnostic-only sampling and replay of RestApiIT's global logger configuration."""
from pathlib import Path

path = Path('seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/java/org/apache/seatunnel/engine/e2e/BackpressureSlowSinkIT.java')
source = path.read_text()

def replace_once(before, after):
    global source
    assert source.count(before) == 1, before
    source = source.replace(before, after, 1)

replace_once('    private ClientJobProxy jobProxy;\n', '''    private ClientJobProxy jobProxy;
    private java.util.concurrent.ScheduledExecutorService diagnosticSampler;

    /** Diagnostic-only snapshots of source/barrier/sink stacks and cumulative GC time. */
    private void sampleDiagnosticThreads() {
        java.lang.management.ThreadMXBean threads = java.lang.management.ManagementFactory.getThreadMXBean();
        StringBuilder sample = new StringBuilder("[DEBUG-backpressure] sample timestamp=")
                .append(System.currentTimeMillis()).append(" liveThreads=").append(threads.getThreadCount());
        for (java.lang.management.GarbageCollectorMXBean gc : java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()) {
            sample.append(" gc=").append(gc.getName()).append(":count=").append(gc.getCollectionCount())
                    .append(":ms=").append(gc.getCollectionTime());
        }
        for (java.lang.management.ThreadInfo info : threads.dumpAllThreads(false, false)) {
            if (info == null) { continue; }
            boolean relevant = false;
            for (StackTraceElement frame : info.getStackTrace()) {
                if (frame.getClassName().endsWith("FakeSourceReader")
                        || (frame.getClassName().endsWith("SourceFlowLifeCycle") && frame.getMethodName().equals("triggerBarrier"))
                        || frame.getClassName().endsWith("InMemorySinkWriter")) {
                    relevant = true;
                }
            }
            if (!relevant) { continue; }
            sample.append("\\n[DEBUG-backpressure] thread=").append(info.getThreadName())
                    .append(" state=").append(info.getThreadState()).append(" lock=").append(info.getLockName())
                    .append(" owner=").append(info.getLockOwnerName()).append(" ownerId=").append(info.getLockOwnerId());
            for (StackTraceElement frame : info.getStackTrace()) {
                sample.append("\\n[DEBUG-backpressure] at ").append(frame);
            }
        }
        System.out.println(sample);
    }
''')
replace_once('    void beforeEach() throws Exception {\n', '''    void beforeEach() throws Exception {
        if ("restapi".equals(System.getProperty("seatunnel.diagnostic.logging"))) {
            org.apache.logging.log4j.core.LoggerContext loggerContext =
                    (org.apache.logging.log4j.core.LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
            loggerContext.setConfigLocation(getClass().getClassLoader().getResource("job-log-file/log4j2.properties").toURI());
        }
        System.out.println("[DEBUG-backpressure] logging=" + System.getProperty("seatunnel.diagnostic.logging"));
        diagnosticSampler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "diagnostic-backpressure-sampler");
            thread.setDaemon(true);
            return thread;
        });
        diagnosticSampler.scheduleAtFixedRate(this::sampleDiagnosticThreads, 0, 5, TimeUnit.SECONDS);
''')
replace_once('    void afterEach() {\n', '''    void afterEach() {
        if (diagnosticSampler != null) {
            diagnosticSampler.shutdownNow();
        }
''')
replace_once('            completedSamples.add(getLong(counts, "completed"));\n', '''            completedSamples.add(getLong(counts, "completed"));
            System.out.println("[DEBUG-backpressure] timestamp=" + System.currentTimeMillis()
                    + " checkpointCounts=" + counts);
''')
path.write_text(source)
print('Applied diagnostic sampling; original assertions, timing and job config are unchanged.')

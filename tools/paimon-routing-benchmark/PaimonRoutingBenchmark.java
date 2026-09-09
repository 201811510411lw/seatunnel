package benchmark;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SinkWriteRouting;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowConverter;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.TableWrite;

import com.sun.management.ThreadMXBean;

import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class PaimonRoutingBenchmark {
    private static volatile long blackhole;

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 9) {
            throw new IllegalArgumentException(
                    "variant sha fork parallelism fields rows warmups batches iterations");
        }
        String variant = arguments[0];
        String sha = arguments[1];
        int fork = positive(arguments[2]);
        int parallelism = positive(arguments[3]);
        int fields = positive(arguments[4]);
        int rowCount = positive(arguments[5]);
        int warmups = positive(arguments[6]);
        int batches = positive(arguments[7]);
        int iterations = positive(arguments[8]);
        if ((parallelism != 1 && parallelism != 2) || (fields != 4 && fields != 32)) {
            throw new IllegalArgumentException("Supported scenarios: p=1/2, fields=4/32");
        }
        PrintStream csv = System.out;
        System.setOut(System.err);
        provenance(PaimonSink.class);
        provenance(PaimonSinkWriter.class);
        provenance(RowConverter.class);
        provenance(SinkWriteRouting.class);
        provenance(TableWrite.class);
        ThreadMXBean allocation = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!allocation.isThreadAllocatedMemorySupported()) {
            throw new IllegalStateException("Thread allocation counters are required");
        }
        allocation.setThreadAllocatedMemoryEnabled(true);
        if (!allocation.isCurrentThreadCpuTimeSupported()) {
            throw new IllegalStateException("Thread CPU counters are required");
        }
        allocation.setThreadCpuTimeEnabled(true);
        long threadId = Thread.currentThread().getId();
        try (Fixture fixture = new Fixture(parallelism, fields, rowCount, iterations)) {
            provenance(fixture.routing.getClass());
            csv.println(
                    "variant,sha,fork,parallelism,fields,rows,iterations,batch,ns_per_row,allocated_bytes_per_row,checksum,route_checksum,cpu_ns_per_row");
            for (int batch = -warmups; batch < batches; batch++) {
                fixture.reset();
                long beforeBytes = allocation.getThreadAllocatedBytes(threadId);
                long beforeCpu = allocation.getCurrentThreadCpuTime();
                long started = System.nanoTime();
                fixture.run(iterations);
                long elapsed = System.nanoTime() - started;
                long cpuElapsed = allocation.getCurrentThreadCpuTime() - beforeCpu;
                long allocated = allocation.getThreadAllocatedBytes(threadId) - beforeBytes;
                long checksum = fixture.verify();
                blackhole = checksum;
                if (batch >= 0) {
                    csv.printf(
                            Locale.ROOT,
                            "%s,%s,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%d,%d,%.3f%n",
                            variant,
                            sha,
                            fork,
                            parallelism,
                            fields,
                            rowCount,
                            iterations,
                            batch + 1,
                            (double) elapsed / iterations,
                            (double) allocated / iterations,
                            checksum,
                            fixture.routeChecksum,
                            (double) cpuElapsed / iterations);
                }
            }
        }
    }

    private static int positive(String value) {
        int parsed = Integer.parseInt(value);
        if (parsed < 1) {
            throw new IllegalArgumentException("Expected a positive integer: " + value);
        }
        return parsed;
    }

    private static void provenance(Class<?> type) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (java.io.InputStream stream =
                type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
            if (stream == null) {
                throw new IllegalStateException("Missing class bytes: " + type.getName());
            }
            byte[] buffer = new byte[8192];
            int count;
            while ((count = stream.read(buffer)) != -1) {
                digest.update(buffer, 0, count);
            }
        }
        StringBuilder hex = new StringBuilder();
        for (byte value : digest.digest()) {
            hex.append(String.format(Locale.ROOT, "%02x", value & 255));
        }
        System.err.println(
                "class="
                        + type.getName()
                        + " sha256="
                        + hex
                        + " source="
                        + type.getProtectionDomain().getCodeSource().getLocation());
    }

    private static final class Fixture implements AutoCloseable {
        private final PaimonCatalog catalog;
        private final List<PaimonSinkWriter> writers = new ArrayList<>();
        private final List<ConsumingWrite> consumers = new ArrayList<>();
        private final SeaTunnelRow[] rows;
        private final SinkWriteRouting routing;
        private final long[] expectedChecksums;
        private final long[] expectedCounts;
        private final long routeChecksum;

        private Fixture(int parallelism, int fields, int rowCount, int iterations)
                throws Exception {
            Path warehouse = Files.createTempDirectory("paimon-routing-benchmark-");
            System.err.println("warehouse=" + warehouse);
            TablePath tablePath = TablePath.of("benchmark", "events");
            Map<String, Object> properties = new HashMap<>();
            properties.put("warehouse", warehouse.toString());
            properties.put("plugin_name", "Paimon");
            properties.put("database", "benchmark");
            properties.put("table", "events");
            Map<String, String> writeProperties = new HashMap<>();
            writeProperties.put("bucket", "8");
            writeProperties.put("bucket-key", "field_0");
            properties.put("paimon.table.write-props", writeProperties);
            ReadonlyConfig config = ReadonlyConfig.fromMap(properties);
            catalog = new PaimonCatalog("benchmark_catalog", config);
            catalog.open();
            catalog.createDatabase(tablePath, false);
            TableSchema.Builder schema = TableSchema.builder();
            for (int column = 0; column < fields; column++) {
                SeaTunnelDataType<?> type =
                        column == 0
                                ? BasicType.INT_TYPE
                                : column % 2 == 0 ? new DecimalType(18, 2) : BasicType.STRING_TYPE;
                schema.column(
                        PhysicalColumn.of(
                                "field_" + column, type, (Long) null, column > 1, null, null));
            }
            schema.primaryKey(PrimaryKey.of("pk", Arrays.asList("field_0", "field_1")));
            CatalogTable table =
                    CatalogTable.of(
                            TableIdentifier.of("benchmark_catalog", "benchmark", "events"),
                            schema.build(),
                            new HashMap<>(),
                            Collections.singletonList("field_1"),
                            "routing benchmark");
            catalog.createTable(tablePath, table, false);
            FileStoreTable physicalTable = (FileStoreTable) catalog.getPaimonTable(tablePath);
            JobContext job = new JobContext(1L);
            job.setJobMode(JobMode.STREAMING);
            job.setEnableCheckpoint(true);
            Field tableWrite = PaimonSinkWriter.class.getDeclaredField("tableWrite");
            tableWrite.setAccessible(true);
            for (int writerIndex = 0; writerIndex < parallelism; writerIndex++) {
                PaimonSink sink = new PaimonSink(config, table);
                sink.setJobContext(job);
                sink.getWriteRouting();
                PaimonSinkWriter writer =
                        sink.createWriter(new DefaultSinkWriterContext(writerIndex, parallelism));
                writers.add(writer);
                ((TableWrite) tableWrite.get(writer)).close();
                ConsumingWrite consumer = new ConsumingWrite(fields);
                tableWrite.set(writer, consumer);
                consumers.add(consumer);
            }
            PaimonSink upstream = new PaimonSink(config, table);
            upstream.setJobContext(job);
            routing = upstream.getWriteRouting().get();
            FixedBucketRowKeyExtractor oracle =
                    new FixedBucketRowKeyExtractor(physicalTable.schema());
            rows = new SeaTunnelRow[rowCount];
            int[] owners = new int[rowCount];
            long[] digests = new long[rowCount];
            long routingDigest = 0;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object[] values = new Object[fields];
                GenericRow expected = new GenericRow(fields);
                for (int column = 0; column < fields; column++) {
                    Object value = inputValue(rowIndex, column);
                    values[column] = value;
                    expected.setField(
                            column,
                            value instanceof String
                                    ? BinaryString.fromString((String) value)
                                    : value instanceof BigDecimal
                                            ? Decimal.fromBigDecimal((BigDecimal) value, 18, 2)
                                            : value);
                }
                SeaTunnelRow row = new SeaTunnelRow(values);
                row.setRowKind(RowKind.values()[rowIndex % RowKind.values().length]);
                expected.setRowKind(
                        org.apache.paimon.types.RowKind.valueOf(row.getRowKind().name()));
                rows[rowIndex] = row;
                oracle.setRecord(expected);
                int owner =
                        ChannelComputer.select(oracle.partition(), oracle.bucket(), parallelism);
                if (routing.route(row, parallelism) != owner) {
                    throw new AssertionError("Routing differs from Paimon oracle at " + rowIndex);
                }
                owners[rowIndex] = owner;
                digests[rowIndex] = digest(expected, fields);
                routingDigest = routingDigest * 31 + owner;
            }
            routeChecksum = routingDigest;
            expectedChecksums = new long[parallelism];
            expectedCounts = new long[parallelism];
            int position = 0;
            for (int iteration = 0; iteration < iterations; iteration++) {
                int owner = owners[position];
                expectedChecksums[owner] = expectedChecksums[owner] * 31 + digests[position];
                expectedCounts[owner]++;
                position = position + 1 == rows.length ? 0 : position + 1;
            }
            for (long count : expectedCounts) {
                if (count == 0) {
                    throw new IllegalArgumentException(
                            "Increase rows/iterations to exercise every writer");
                }
            }
        }

        private void reset() {
            for (ConsumingWrite consumer : consumers) {
                consumer.checksum = 0;
                consumer.count = 0;
            }
        }

        private void run(int iterations) throws Exception {
            int position = 0;
            for (int iteration = 0; iteration < iterations; iteration++) {
                SeaTunnelRow row = rows[position];
                writers.get(routing.route(row, writers.size())).write(row);
                position = position + 1 == rows.length ? 0 : position + 1;
            }
        }

        private long verify() {
            long combined = 0;
            for (int writerIndex = 0; writerIndex < consumers.size(); writerIndex++) {
                ConsumingWrite consumer = consumers.get(writerIndex);
                if (consumer.count != expectedCounts[writerIndex]
                        || consumer.checksum != expectedChecksums[writerIndex]) {
                    throw new AssertionError("Writer checksum/count mismatch: " + writerIndex);
                }
                combined = combined * 31 + consumer.checksum;
            }
            return combined;
        }

        @Override
        public void close() throws Exception {
            try {
                for (PaimonSinkWriter writer : writers) {
                    writer.close();
                }
            } finally {
                catalog.close();
            }
        }
    }

    private static long digest(InternalRow row, int fields) {
        long result = row.getRowKind().toByteValue();
        for (int column = 0; column < fields; column++) {
            long value =
                    row.isNullAt(column)
                            ? -1
                            : column == 0
                                    ? row.getInt(column)
                                    : column % 2 == 0
                                            ? row.getDecimal(column, 18, 2).toUnscaledLong()
                                            : row.getString(column).hashCode();
            result = result * 31 + value;
        }
        return result;
    }

    private static Object inputValue(int rowIndex, int column) {
        if (column == 0) {
            return rowIndex;
        }
        if (column == 1) {
            return "partition-" + rowIndex % 7;
        }
        if ((rowIndex + column) % 5 == 0) {
            return null;
        }
        if (column % 2 == 0) {
            return BigDecimal.valueOf(rowIndex * 101L + column, 2);
        }
        return "payload-" + rowIndex + "-" + column + "-中文";
    }

    private static final class ConsumingWrite implements TableWrite {
        private final int fields;
        private long checksum;
        private long count;

        private ConsumingWrite(int fields) {
            this.fields = fields;
        }

        @Override
        public void write(InternalRow row) {
            checksum = checksum * 31 + digest(row, fields);
            count++;
        }

        @Override
        public void close() {}

        @Override
        public TableWrite withIOManager(IOManager manager) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableWrite withMemoryPool(MemorySegmentPool pool) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void withInsertOnly(boolean insertOnly) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BinaryRow getPartition(InternalRow row) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getBucket(InternalRow row) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(InternalRow row, int bucket) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeBundle(BinaryRow partition, int bucket, BundleRecords records) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void compact(BinaryRow partition, int bucket, boolean fullCompaction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableWrite withMetricRegistry(MetricRegistry registry) {
            throw new UnsupportedOperationException();
        }
    }
}

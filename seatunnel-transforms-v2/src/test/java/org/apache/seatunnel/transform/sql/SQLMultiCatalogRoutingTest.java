package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

class SQLMultiCatalogRoutingTest {

    private static final LocalDateTime ADD_TIME = LocalDateTime.of(2026, 9, 9, 9, 32, 10);

    @Test
    void testSingleUnmatchedTablePassesThrough() {
        CatalogTable table = table("daily");
        SQLMultiCatalogFlatMapTransform transform =
                transform(Collections.singletonList(table), "never_matches", "select * from dual");
        SeaTunnelRow input = row(table);
        Assertions.assertSame(input, transform.flatMap(input).get(0));
        Assertions.assertEquals(
                table.getTableSchema(), transform.getProducedCatalogTables().get(0).getTableSchema());
    }

    @Test
    void testThreeDisjointSqlTransformsPreserveRowsAndSchemas() {
        CatalogTable daily = table("daily");
        CatalogTable monthly = table("monthly");
        CatalogTable plain = table("plain");
        List<CatalogTable> tables = Arrays.asList(daily, monthly, plain);
        SQLMultiCatalogFlatMapTransform dailyTransform =
                transform(
                        tables,
                        Pattern.quote(tableId(daily)),
                        "SELECT 'warehouse' AS source_system, "
                                + "COALESCE(FORMATDATETIME(addTime, 'yyyy-MM-dd'), '1970-01-01') AS dt, * FROM dual");
        SQLMultiCatalogFlatMapTransform monthlyTransform =
                transform(
                        dailyTransform.getProducedCatalogTables(),
                        Pattern.quote(tableId(monthly)),
                        "SELECT 'warehouse' AS source_system, "
                                + "COALESCE(FORMATDATETIME(addTime, 'yyyy-MM'), '1970-01') AS dt, * FROM dual");
        SQLMultiCatalogFlatMapTransform plainTransform =
                transform(
                        monthlyTransform.getProducedCatalogTables(),
                        Pattern.quote(tableId(plain)),
                        "SELECT 'warehouse' AS source_system, * FROM dual");

        for (CatalogTable table : tables) {
            SeaTunnelRow input = row(table);
            List<SeaTunnelRow> dailyRows = dailyTransform.flatMap(input);
            Assertions.assertEquals(1, dailyRows.size());
            List<SeaTunnelRow> monthlyRows = monthlyTransform.flatMap(dailyRows.get(0));
            Assertions.assertEquals(1, monthlyRows.size());
            List<SeaTunnelRow> output = plainTransform.flatMap(monthlyRows.get(0));
            Assertions.assertEquals(1, output.size());
            SeaTunnelRow result = output.get(0);
            Assertions.assertEquals(tableId(table), result.getTableId());
            Assertions.assertEquals(input.getRowKind(), result.getRowKind());
            Object[] expected =
                    table == plain
                            ? new Object[] {"warehouse", 7, ADD_TIME}
                            : new Object[] {
                                "warehouse",
                                table == daily ? "2026-09-09" : "2026-09",
                                7,
                                ADD_TIME
                            };
            Assertions.assertArrayEquals(expected, result.getFields());
        }
        List<CatalogTable> outputTables = plainTransform.getProducedCatalogTables();
        for (int index = 0; index < outputTables.size(); index++) {
            String[] expected =
                    index == 2
                            ? new String[] {"source_system", "id", "addTime"}
                            : new String[] {"source_system", "dt", "id", "addTime"};
            Assertions.assertArrayEquals(
                    expected, outputTables.get(index).getSeaTunnelRowType().getFieldNames());
        }
    }

    private static CatalogTable table(String name) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "addTime"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE});
        return CatalogTableUtil.getCatalogTable("test", "wms", null, name, rowType);
    }

    private static String tableId(CatalogTable table) {
        return table.getTableId().toTablePath().toString();
    }

    private static SeaTunnelRow row(CatalogTable table) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {7, ADD_TIME});
        row.setTableId(tableId(table));
        return row;
    }

    private static SQLMultiCatalogFlatMapTransform transform(
            List<CatalogTable> tables, String regex, String query) {
        Map<String, Object> config = new HashMap<>();
        config.put("table_match_regex", regex);
        config.put("query", query);
        return new SQLMultiCatalogFlatMapTransform(tables, ReadonlyConfig.fromMap(config));
    }
}

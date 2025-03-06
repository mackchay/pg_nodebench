package com.haskov.bitmapscan;

import com.haskov.Cmd;
import com.haskov.PlanAnalyzer;
import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.costs.scan.BitmapScanCostCalculator;
import com.haskov.json.JsonOperations;
import com.haskov.json.PgJsonPlan;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestBitmapScanCost {
    private final static String expectedNodeType = "Bitmap Heap Scan";
    private final static String filePath = "testplans/bitmapscan.json";

    public void test(long size, int queryCount) {
        String argArray = "-h localhost -j " + filePath + " -S " + size + " -q " + queryCount;
        Configuration conf = Cmd.args(argArray.split(" "));
        V2.init(conf);
        PlanAnalyzer analyzer = new PlanAnalyzer(conf.tableSize, conf.plan);
        List<TableBuildResult> tableScripts = analyzer.prepareTables();
        List<String> tables = List.of(tableScripts.getFirst().tableName(), tableScripts.getLast().tableName());
        generateQuery(tables, size);
        V2.closeConnection();
    }

    @Test
    public void testBitmapScan() {
        //test(200, 200);
        test(500, 500);
        test(1000, 200);
        test(5000, 50);
        test(10000, 50);
        test(100000, 100);
    }

    private void generateQuery(List<String> tables, long size) {
        String table = tables.get(0);

        Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(table);
        List<String> columns = new ArrayList<>(columnsAndTypes.keySet());

        double sel = 0.22;
        long tuples = (long) (size * sel);

        List<String> nonIndexColumns = new ArrayList<>();
        List<String> indexColumns = new ArrayList<>();

        QueryBuilder qb = new QueryBuilder();
        for (String column : columns) {
            if (SQLUtils.hasIndexOnColumn(table, column)) {
                indexColumns.add(column);
            } else {
                nonIndexColumns.add(column);
                qb.select(table + "." + column);
                qb.where(table + "." + column + " < " + tuples);
            }
        }

        qb.select(table + "." + indexColumns.getFirst()).from(table);
        qb.where(table + "." + indexColumns.getFirst() + " < " + tuples + " and " +
                table + "." + indexColumns.getFirst() + " >= 0");

        String query = qb.build();


        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query), expectedNodeType);
        System.out.println(query);
        V2.explain(V2.log, query);
        double expectedCost = Objects.requireNonNull(JsonOperations.
                        findNode(JsonOperations.explainResultsJson(query), expectedNodeType)).
                getJson().get("Total Cost").getAsDouble();

        BitmapScanCostCalculator costCalculator = new BitmapScanCostCalculator(table, indexColumns.getFirst());

        double actualCost = costCalculator.calculateCost(2, nonIndexColumns.size(), sel);
        double bitmapIndexCost = costCalculator.calculateIndexCost(2, sel);

        Assert.assertEquals(expectedCost, actualCost, 0.01 * expectedCost);
    }
}

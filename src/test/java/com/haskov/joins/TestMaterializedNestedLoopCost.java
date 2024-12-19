package com.haskov.joins;

import com.haskov.Cmd;
import com.haskov.PlanAnalyzer;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.costs.JoinCostCalculator;
import com.haskov.costs.ScanCostCalculator;
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

public class TestMaterializedNestedLoopCost {
    private final static String expectedNodeType = "Nested Loop";
    private final static String filePath = "testplans/nestedloop_materialize.json";

    public void test(long size, int queryCount) {
        String argArray = "-h localhost -j " + filePath + " -S " + size + " -q " + queryCount;
        Configuration conf = Cmd.args(argArray.split(" "));
        V2.init(conf);
        PlanAnalyzer analyzer = new PlanAnalyzer(conf.tableSize, conf.plan);
        List<TableBuildResult> tableScripts = analyzer.prepareTables();
        List<String> tables = List.of(tableScripts.getFirst().tableName(), tableScripts.getLast().tableName());
        List<String> nonIndexedColumns = new ArrayList<>();
        for (String table : tables) {
            Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(table);
            for (String column : columnsAndTypes.keySet()) {
                if (!SQLUtils.hasIndexOnColumn(table, column)) {
                    nonIndexedColumns.add(column);
                    break;
                }
            }
        }

        String query = "select " + tables.getFirst() + "." + nonIndexedColumns.getFirst() + ","
                + tables.getLast() + "." + nonIndexedColumns.getLast()
                + " from " + tables.getFirst() + " join "
                + tables.getLast() + " on " + tables.getFirst() + "." + nonIndexedColumns.getFirst()
                + "=" + tables.getLast() + "." + nonIndexedColumns.getLast()
                + " where " + tables.getFirst() + "." + nonIndexedColumns.getFirst() + " < 4 and "
                + tables.getLast() + "." + nonIndexedColumns.getLast() + " < 4 ";
        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query), expectedNodeType);
        System.out.println(query);
        V2.explain(V2.log, query);
        double expectedCost = Objects.requireNonNull(JsonOperations.
                        findNode(JsonOperations.explainResultsJson(query), expectedNodeType)).
                getJson().get("Total Cost").getAsDouble();
        double sel = (double) 4 / size;

        double scanCost = ScanCostCalculator.calculateSeqScanCost(tables.getFirst(), 1);
        double actualCost = JoinCostCalculator.calculateMaterializedNestedLoopCost(
                tables.getFirst(),
                tables.getLast(),
                ScanCostCalculator.calculateSeqScanCost(tables.getFirst(), 1),
                ScanCostCalculator.calculateSeqScanCost(tables.getLast(), 1),
                sel,
                sel
        );

        Assert.assertEquals(expectedCost, actualCost, expectedCost * 0.005);
    }

    @Test
    public void testNestedLoop() {
        test(500, 500);
        test(1000, 200);
        test(5000, 50);
        test(10000, 100);
        test(50000, 100);
        test(100000, 50);
    }
}

package com.haskov.joins;

import com.haskov.Cmd;
import com.haskov.PlanAnalyzer;
import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.costs.JoinCostCalculator;
import com.haskov.costs.ScanCostCalculator;
import com.haskov.json.JsonOperations;
import com.haskov.json.PgJsonPlan;
import com.haskov.types.JoinData;
import com.haskov.types.JoinType;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestMergeJoinCost {
    private final static String expectedNodeType = "Merge Join";
    private final static String filePath = "testplans/mergejoin.json";

    public void test(long size, int queryCount) {
        String argArray = "-h localhost -j " + filePath + " -S " + size + " -q " + queryCount;
        Configuration conf = Cmd.args(argArray.split(" "));
        V2.init(conf);
        PlanAnalyzer analyzer = new PlanAnalyzer(conf.tableSize, conf.plan);
        long tuples = (long) (size * 0.35);

        List<TableBuildResult> tableScripts = analyzer.prepareTables();
        List<String> tables = List.of(tableScripts.getFirst().tableName(), tableScripts.getLast().tableName());
        List<String> indexedColumns = new ArrayList<>();
        for (String table : tables) {
            Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(table);
            for (String column : columnsAndTypes.keySet()) {
                if (SQLUtils.hasIndexOnColumn(table, column)) {
                    indexedColumns.add(column);
                    break;
                }
            }
        }

        String query = new QueryBuilder().select(tables.getFirst() + "." + indexedColumns.getFirst(),
                        tables.getLast() + "." + indexedColumns.getLast()).join(
                        new JoinData(
                                tables.getLast(),
                                tables.getFirst(),
                                JoinType.USUAL,
                                indexedColumns.getLast(),
                                indexedColumns.getFirst())
                ).where(tables.getFirst() + "." + indexedColumns.getFirst() + " < " + tuples
                        + " and "
                        + tables.getLast() + "." + indexedColumns.getLast() + " < " + tuples
                        + " and " + tables.getFirst() + "." + indexedColumns.getFirst() + " >= 0 ")
                .orderBy(tables.getFirst() + "." + indexedColumns.getFirst(),
                        tables.getLast() + "." + indexedColumns.getLast()).
                from(tables.getLast()).build();
        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query), expectedNodeType);
        System.out.println(query);
        V2.explain(V2.log, query);
        double expectedCost = Objects.requireNonNull(JsonOperations.
                        findNode(JsonOperations.explainResultsJson(query), expectedNodeType)).
                getJson().get("Total Cost").getAsDouble();
        double sel = Math.min((double) tuples / size, 1);

        double scanCost = ScanCostCalculator.calculateIndexOnlyScanCost(tables.getLast(), indexedColumns.getLast(),
                2, 0, sel);
        double doubleScanCost = ScanCostCalculator.calculateIndexOnlyScanCost(tables.getLast(), indexedColumns.getLast(),
                1, 0, sel);
        double actualCost = JoinCostCalculator.calculateMergeJoinCost(
                tables.getLast(),
                tables.getFirst(),
                ScanCostCalculator.calculateIndexOnlyScanCost(tables.getLast(), indexedColumns.getLast(),
                        2, 0, sel),
                ScanCostCalculator.calculateIndexOnlyScanCost(tables.getFirst(), indexedColumns.getFirst(),
                        1, 0, sel),
                sel,
                sel,
                2,
                1
        );

        Assert.assertEquals(expectedCost, actualCost, 0.01 * actualCost);
    }

    @Test
    public void testMergeJoin() {
        //test(200, 50);
        test(500, 500);
        test(5000, 500);
        test(10000, 200);
        test(100000, 50);
    }
}

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
import com.haskov.types.JoinNodeType;
import com.haskov.types.JoinType;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestNestedLoopMaxTuples {
    private final static String expectedNodeType = "Nested Loop";
    private final static String filePath = "testplans/nestedloop.json";

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
        double sel = Math.min((double) 50000 / size, 1);

        ScanCostCalculator costCalculator = new ScanCostCalculator();

        double scanCost = costCalculator.calculateSeqScanCost(tables.getFirst(), 1);
        double actualCost = JoinCostCalculator.calculateHashJoinCost(
                tables.getFirst(),
                tables.getLast(),
                costCalculator.calculateSeqScanCost(tables.getFirst(), 1),
                costCalculator.calculateSeqScanCost(tables.getLast(), 1),
                sel,
                sel,
                0,
                1,
                1
        );

        long maxTuples = new JoinCostCalculator().calculateTuplesRange(
                tables.getFirst(),
                tables.getLast(),
                costCalculator.calculateSeqScanCost(tables.getFirst(), 1),
                costCalculator.calculateSeqScanCost(tables.getLast(), 1),
                0,
                1,
                1,
                JoinNodeType.NESTED_LOOP
        ).getLeft();

        String query = new QueryBuilder().select(tables.getFirst() + "." + nonIndexedColumns.getFirst(),
                        tables.getLast() + "." + nonIndexedColumns.getLast()).join(
                        new JoinData(
                                tables.getLast(),
                                tables.getFirst(),
                                JoinType.USUAL,
                                nonIndexedColumns.getLast(),
                                nonIndexedColumns.getFirst())
                ).where(tables.getFirst() + "." + nonIndexedColumns.getFirst() + " <= " + (maxTuples)
                        + " and "
                        + tables.getLast() + "." + nonIndexedColumns.getLast() + " <= " + (maxTuples)
                        + " and " + tables.getFirst() + "." + nonIndexedColumns.getFirst() + " >= 0 "
                        + " and " + tables.getLast() + "." + nonIndexedColumns.getLast() + " >= 0").
                from(tables.getLast()).build();

        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query), expectedNodeType);
        System.out.println(query);
        V2.explain(V2.log, query);

        Assert.assertNotEquals(plan, null);
    }

    @Test
    public void testNestedLoop() {
        test(500, 500);
        test(1000, 500);
        test(5000, 500);
        test(10000, 200);
        //test(100000, 50);
    }
}

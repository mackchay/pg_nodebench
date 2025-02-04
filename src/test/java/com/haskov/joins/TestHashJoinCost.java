package com.haskov.joins;

import com.haskov.Cmd;
import com.haskov.PlanAnalyzer;
import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.costs.join.HashJoinCostCalculator;
import com.haskov.costs.join.NestedLoopJoinCostCalculator;
import com.haskov.costs.scan.JoinCostCalculator;
import com.haskov.costs.scan.SeqScanCostCalculator;
import com.haskov.json.JsonOperations;
import com.haskov.json.PgJsonPlan;
import com.haskov.types.JoinData;
import com.haskov.types.JoinType;
import com.haskov.types.TableBuildResult;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestHashJoinCost {
    private final static String expectedNodeType = "Hash Join";
    private final static String filePath = "testplans/hashjoin.json";

    public void test(long size, int queryCount) {
        String argArray = "-h localhost -j " + filePath + " -S " + size + " -q " + queryCount;
        Configuration conf = Cmd.args(argArray.split(" "));
        V2.init(conf);
        PlanAnalyzer analyzer = new PlanAnalyzer(conf.tableSize, conf.plan);
        List<TableBuildResult> tableScripts = analyzer.prepareTables();
        List<String> tables = List.of(tableScripts.getFirst().tableName(), tableScripts.getLast().tableName());
        generateQuery(tables, size);
    }

    @Test
    public void testHashJoin() {
        test(200, 200);
        test(500, 500);
        test(1000, 200);
        test(5000, 50);
        test(10000, 100);
    }

    private void generateQuery(List<String> tables, long size) {
        String parentTable = tables.get(0);
        String childTable = tables.get(1);

        Map<String, String> columnsParent = V2.getColumnsAndTypes(parentTable);
        List<String> parentTableColumns = new ArrayList<>(columnsParent.keySet());

        Map<String, String> columnsChild = V2.getColumnsAndTypes(childTable);
        List<String> childTableColumns = new ArrayList<>(columnsChild.keySet());

        double sel = 0.9;
        long tuples = (long) (size * sel);

        QueryBuilder qb = new QueryBuilder();
        qb.select(parentTable + "." + parentTableColumns.getFirst(),
                childTable + "." + childTableColumns.getFirst()).join(
                new JoinData(
                        parentTable,
                        childTable,
                        JoinType.INNER,
                        parentTableColumns.getFirst(),
                        childTableColumns.getFirst())
        ).from(parentTable);

        StringBuilder whereConditions = new StringBuilder();
        for (String column : childTableColumns) {
            whereConditions.append(childTable).append(".").append(column).append(" < ").append(tuples);
            whereConditions.append(" and ");
        }
        for (String column : parentTableColumns) {
            whereConditions.append(parentTable).append(".").append(column).append(" < ").append(tuples);
            if (parentTableColumns.indexOf(column) != parentTableColumns.size() - 1) {
                whereConditions.append(" and ");
            }
        }
        qb.where(whereConditions.toString());
        String query = qb.build();



        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query), expectedNodeType);
        System.out.println(query);
        V2.explain(V2.log, query);
        double expectedCost = Objects.requireNonNull(JsonOperations.
                        findNode(JsonOperations.explainResultsJson(query), expectedNodeType)).
                getJson().get("Total Cost").getAsDouble();
        SeqScanCostCalculator costCalculator = new SeqScanCostCalculator(parentTable);
        SeqScanCostCalculator costCalculator1 = new SeqScanCostCalculator(childTable);

        double parentCost = costCalculator.calculateCost(parentTableColumns.size());
        double childCost = costCalculator1.calculateCost(childTableColumns.size());

        double innerCost, outerCost;
        String innerTable, outerTable;
        int innerConditions, outerConditions;

        if (parentCost > childCost) {
            innerCost = parentCost;
            outerCost = childCost;
            innerTable = parentTable;
            outerTable = childTable;
            innerConditions = parentTableColumns.size();
            outerConditions = childTableColumns.size();
        } else {
            innerCost = childCost;
            outerCost = parentCost;
            innerTable = childTable;
            outerTable = parentTable;
            innerConditions = childTableColumns.size();
            outerConditions = parentTableColumns.size();
        }

        HashJoinCostCalculator joinCostCalculator = new HashJoinCostCalculator(innerTable, outerTable);

        double actualCost = joinCostCalculator.calculateCost(
                innerCost,
                outerCost,
                sel,
                sel,
                innerConditions,
                outerConditions,
                0
        );

        Assert.assertEquals(expectedCost, actualCost, 0.03 * expectedCost);
    }
}

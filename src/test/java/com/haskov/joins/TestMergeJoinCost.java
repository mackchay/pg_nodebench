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
        List<TableBuildResult> tableScripts = analyzer.prepareTables();
        List<String> tables = List.of(tableScripts.getFirst().tableName(), tableScripts.getLast().tableName());
        generateQuery(tables, size);
    }

    @Test
    public void testNestedLoop() {
        //test(200, 200);
        test(500, 500);
        test(1000, 200);
        test(5000, 50);
        test(10000, 100);
    }

    private void generateQuery(List<String> tables, long size) {
        String parentTable = tables.get(0);
        String childTable = tables.get(1);

        Map<String, String> columnsParent = V2.getColumnsAndTypes(parentTable);
        List<String> parentTableColumns = new ArrayList<>(columnsParent.keySet()).subList(0, 1);

        Map<String, String> columnsChild = V2.getColumnsAndTypes(childTable);
        List<String> childTableColumns = new ArrayList<>(columnsChild.keySet()).subList(0, 1);

        double sel = 0.2;
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

        ScanCostCalculator costCalculator = new ScanCostCalculator();

        double parentCost = costCalculator.calculateIndexOnlyScanCost(parentTable, parentTableColumns.getFirst(),
                parentTableColumns.size(), 0, sel);
        double childCost = costCalculator.calculateIndexOnlyScanCost(childTable, childTableColumns.getFirst(),
                childTableColumns.size(), 0, sel);

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

        double actualCost = JoinCostCalculator.calculateMergeJoinCost(
                innerTable,
                outerTable,
                innerCost,
                outerCost,
                sel,
                sel,
                innerConditions,
                outerConditions
        );

        Assert.assertEquals(expectedCost, actualCost, 0.1);
    }
}

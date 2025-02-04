package com.haskov.indexonlyscan;

import com.haskov.Cmd;
import com.haskov.PlanAnalyzer;
import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.costs.scan.IndexOnlyScanCostCalculator;
import com.haskov.json.JsonOperations;
import com.haskov.json.PgJsonPlan;
import com.haskov.types.TableBuildResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestIndexOnlyScanCost {
    private final static String expectedNodeType = "Index Only Scan";
    private final static String filePath = "testplans/indexonlyscan.json";

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
    public void testIndexOnlyScan() {
        //test(200, 200);
        test(500, 500);
        test(1000, 200);
        test(5000, 50);
        test(10000, 50);
        test(100000, 100);
    }

    private void generateQuery(List<String> tables, long size) {
        String table = tables.get(0);

        Map<String, String> columns = V2.getColumnsAndTypes(table);
        String column = new ArrayList<>(columns.keySet()).getFirst();

        double sel = 0.25;
        long tuples = (long) (size * sel);

        QueryBuilder qb = new QueryBuilder();
        qb.select(table + "." + column).from(table);

        qb.where(table + "." + column + " < " + tuples);
        String query = qb.build();



        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query), expectedNodeType);
        System.out.println(query);
        V2.explain(V2.log, query);
        double expectedCost = Objects.requireNonNull(JsonOperations.
                        findNode(JsonOperations.explainResultsJson(query), expectedNodeType)).
                getJson().get("Total Cost").getAsDouble();


        IndexOnlyScanCostCalculator costCalculator = new IndexOnlyScanCostCalculator(table, column);

        double actualCost = costCalculator.calculateCost(1, sel);

        Assert.assertEquals(expectedCost, actualCost, 0.1);
    }
}

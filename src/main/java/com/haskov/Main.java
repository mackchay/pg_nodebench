package com.haskov;

import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.costs.ScanCostCalculator;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeFactory;
import com.haskov.test.TestUtils;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        Node node = NodeFactory.createNode(conf.node);
        long loadSize = 1000;
        //testQueryPlanCost();
        String[] queries = LoadGenerator.generate(List.of(node),
                conf.sizeOfTable, loadSize).toArray(new String[0]);
        TestUtils.testQueriesOnNode(queries, conf.node);
    }

    public static void testQueryPlanCost() {
        String tableName = "pg_indexscan";
        String indexedColumn = "x";
        double planCost = ScanCostCalculator.calculateIndexScanCost(tableName, indexedColumn, 4, 2);
        double planCost1 = planCost * ((double) 40 / 10000 + (double) 40 / 10000 + (double) 40 / 10000);
        planCost = ScanCostCalculator.calculateIndexScanCost(tableName, indexedColumn, 2, 2);
        double planCost2 = planCost * ((double) 40 / 10000 + (double) 40 / 10000);
        planCost = ScanCostCalculator.calculateIndexScanCost(tableName, indexedColumn, 2, 0);
        double planCost3 = planCost * ((double) 40 / 10000 + (double) 1 /3 + (double) 1 /3);
        planCost = ScanCostCalculator.calculateIndexOnlyScanCost(tableName, indexedColumn, 4, 0);
        double planCost4 = planCost * ((double) 40 / 10000 + (double) 40 / 10000);
        planCost = ScanCostCalculator.calculateIndexOnlyScanCost(tableName, indexedColumn, 2, 0);
        double planCost5 = planCost * ((double) 40 / 10000);
        System.out.println(planCost1);
        System.out.println(planCost2);
        System.out.println(planCost3);
        System.out.println(planCost4);
        String query1 = "select x,y,z from " + tableName + " where (x > 10) and (x < 50) and (y < 10) and (y > 50) and" +
                "(z < 10) and (z > 50)";
        String query2 = "select x,y,z from " + tableName + " where (x > 10) and (x < 50) and (y < 10) and (y > 50)";
        String query3 = "select x,y,z from " + tableName + " where (x > 10) and (x < 50)";
        String query4 = "select x,z from " + tableName + " where (x > 10) and (x < 50) and (z > 10) and (z < 50)";
        String query5 = "select x from " + tableName + " where (x > 10) and (x < 50)";
        V2.explain(LoggerFactory.getLogger("Main"), query1);
        V2.explain(LoggerFactory.getLogger("Main"), query2);
        V2.explain(LoggerFactory.getLogger("Main"), query3);
        V2.explain(LoggerFactory.getLogger("Main"), query4);
        V2.explain(LoggerFactory.getLogger("Main"), query5);
        //TestUtils.testQueriesOnNode(new String[]{query}, "IndexScan");
    }
}

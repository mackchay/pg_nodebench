package com.haskov;

import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.costs.ScanCostCalculator;
import com.haskov.json.JsonOperations;
import com.haskov.json.PgJsonPlan;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class TestBitmapScanCost {
    private Configuration initDB(int size) {
        String argArray = "-h localhost -n BitmapScan -S " + size;
        String[] args = List.of(argArray.split(" ")).toArray(new String[0]);
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        return conf;
    }

    private void checkTestBitmapScan(int sizeOfTable, String conditions, int idxConditionCount,
                                     int conditionCount, double selectivity) {
        Configuration conf = initDB(sizeOfTable);
        Node node = NodeFactory.createNode(conf.node);
        List<String> tables = node.prepareTables(conf.sizeOfTable);
        String query = "select * from " + tables.getFirst() + conditions;
        double actualCost = ScanCostCalculator.calculateBitmapHeapAndIndexScanCost(tables.getFirst(), "x",
                idxConditionCount, conditionCount, selectivity);
        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query), "Bitmap Heap Scan");
        V2.explain(LoggerFactory.getLogger("TestBitmapScanCost"), query);
        double expectedCost = Objects.requireNonNull(JsonOperations.
                        findNode(JsonOperations.explainResultsJson(query), "Bitmap Heap Scan")).
                getJson().get("Total Cost").getAsDouble();
        Assert.assertEquals(expectedCost, actualCost, expectedCost * 0.05);
    }

    @Test
    public void testBitmapScan() {
        checkTestBitmapScan(500, " where x < 51", 1, 0,
                (double) 50 / 500);
        checkTestBitmapScan(10000, " where x < 1001", 1, 0,
                (double) 1000 /10000);
        checkTestBitmapScan(100000, " where x < 10001", 1, 0,
                (double) 10000 /100000);
    }

    @Test
    public void testBitmapScanConditions() {
        checkTestBitmapScan(500, " where x > 0 and x < 51", 2, 0,
                (double) 50 /500);
        checkTestBitmapScan(10000, " where x > 0 and x < 1501", 2, 0,
                (double) 1500 /10000);
        checkTestBitmapScan(100000, " where x > 0 and x < 10001", 2, 0,
                (double) 10000 /100000);
        checkTestBitmapScan(100000, " where x > 0 and x < 40001", 2, 0,
                (double) 40000 /100000);
    }

    @Test
    public void testBitmapScanMoreConditions() {
        checkTestBitmapScan(500, " where x > 0 and x < 51 and y > 0 and y < 51",
                2, 2,
                (double) 50 / 500);
        checkTestBitmapScan(100000, " where x > 0 and x < 501 and y > 0 and y < 501",
                2, 2,
                (double) 500 /100000);
        checkTestBitmapScan(100000, " where x > 0 and x < 30001 and y > 0 and y < 30001",
                2, 2,
                (double) 30000/100000);
    }
}

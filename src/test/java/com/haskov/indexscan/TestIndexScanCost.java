package com.haskov.indexscan;

import com.haskov.Cmd;
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

public class TestIndexScanCost {

    private Configuration initDB(int size) {
        String argArray = "-h localhost -j testplans/indexscan.json -S " + size;
        String[] args = List.of(argArray.split(" ")).toArray(new String[0]);
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        return conf;
    }

    //TODO: fix test
    private void checkTestSeqScan(int sizeOfTable, String conditions, int idxConditionCount,
                                  int conditionCount, double selectivity) {
//        Configuration conf = initDB(sizeOfTable);
//        Node node = NodeFactory.createNode("IndexScan");
//        List<String> tables = node.prepareTables(conf.tableSize);
//        String query = "select * from " + tables.getFirst() + conditions;
//        double actualCost = ScanCostCalculator.calculateIndexScanCost(tables.getFirst(), "x",
//                idxConditionCount, conditionCount, selectivity);
//        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query), "Index Scan");
//        V2.explain(LoggerFactory.getLogger("TestIndexScan"), query);
//        double expectedCost = Objects.requireNonNull(JsonOperations.
//                        findNode(JsonOperations.explainResultsJson(query), "Index Scan")).
//                getJson().get("Total Cost").getAsDouble();
//        Assert.assertEquals(expectedCost, actualCost, expectedCost * 0.05);
    }

    @Test
    public void testIndexScan() {
        checkTestSeqScan(500, " where x < 2", 1, 0,
                (double) 1 / 500);
        checkTestSeqScan(10000, " where x < 11", 1, 0,
                (double) 10 /10000);
        checkTestSeqScan(100000, " where x < 1001", 1, 0,
                (double) 1000 /100000);
    }

    @Test
    public void testIndexScanConditions() {
        checkTestSeqScan(500, " where x > 0 and x < 2", 2, 0,
                (double) 1 /500);
        checkTestSeqScan(10000, " where x > 0 and x < 501", 2, 0,
                (double) 500 /10000);
        checkTestSeqScan(100000, " where x > 0 and x < 10001", 2, 0,
                (double) 10000 /100000);
        checkTestSeqScan(100000, " where x > 0 and x < 40001", 2, 0,
                (double) 40000 /100000);
    }

    @Test
    public void testIndexScanMoreConditions() {
        checkTestSeqScan(500, " where x > 0 and x < 2 and y > 0 and y < 2",
                2, 2,
                (double) 1 / 500);
        checkTestSeqScan(100000, " where x > 0 and x < 500 and y > 0 and y < 500",
                2, 2,
                (double) 499 /100000);
        checkTestSeqScan(100000, " where x > 0 and x < 50001 and y > 0 and y < 50001",
                2, 2,
                (double) 50000/100000);
    }
}

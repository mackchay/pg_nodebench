package com.haskov.bitmapscan;

import com.haskov.Cmd;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.costs.ScanCostCalculator;
import com.haskov.json.JsonOperations;
import com.haskov.json.PgJsonPlan;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TestBitmapScanMaxTuples {
    private Configuration initDB(int size) {
        String argArray = "-h localhost -j testplans/bitmapscan.json -S " + size;
        String[] args = List.of(argArray.split(" ")).toArray(new String[0]);
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        return conf;
    }

    //TODO: fix test
    private void checkTestBitmapScan(int sizeOfTable) {
//        Configuration conf = initDB(sizeOfTable);
//        Node node = NodeFactory.createNode("BitmapScan");
//        List<String> tables = node.prepareTables(conf.tableSize);
//        int conditionCount = 2;
//        String query = "select * from " + tables.getFirst();
//        ScanCostCalculator scanCostCalculator = new ScanCostCalculator();
//        Pair<Long, Long> tuples = scanCostCalculator.calculateBitmapIndexScanTuplesRange(
//                tables.getFirst(), "x",
//                conditionCount, 2);
//        query += " where x > 0 and x < " + tuples.getLeft() + " and y > 0 and y < " +
//                tuples.getRight();
//        V2.explain(LoggerFactory.getLogger("TestBitmapScanMaxTuples"), query);
//        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query),
//                "Bitmap Index Scan");
//        PgJsonPlan isCorrectPlan = JsonOperations.
//                findNode(JsonOperations.explainResultsJson(query), "Bitmap Index Scan");
//        Assert.assertTrue(isCorrectPlan != null);
    }

    @Test
    public void testBitmapScanMaxTuples() {
        checkTestBitmapScan(500);
        checkTestBitmapScan(800);
        checkTestBitmapScan(1000);
        checkTestBitmapScan(10000);
        checkTestBitmapScan(100000);
    }
}

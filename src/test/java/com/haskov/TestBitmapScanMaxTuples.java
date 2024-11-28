package com.haskov;

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
        String argArray = "-h localhost -n BitmapScan -S " + size;
        String[] args = List.of(argArray.split(" ")).toArray(new String[0]);
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        return conf;
    }

    private void checkTestIndexScan(int sizeOfTable) {
        Configuration conf = initDB(sizeOfTable);
        Node node = NodeFactory.createNode(conf.node);
        List<String> tables = node.prepareTables(conf.sizeOfTable);
        int conditionCount = 2;
        String query = "select * from " + tables.getFirst();
        ScanCostCalculator scanCostCalculator = new ScanCostCalculator();
        Pair<Long, Long> tuples = scanCostCalculator.calculateBitmapIndexScanTuplesRange(
                tables.getFirst(), "x",
                conditionCount, 2);
        query += " where x > 0 and x < " + tuples.getLeft() + " and y > 0 and y < " +
                tuples.getRight();
        V2.explain(LoggerFactory.getLogger("TestBitmapScanMaxTuples"), query);
        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query),
                "Bitmap Index Scan");
        PgJsonPlan isCorrectPlan = JsonOperations.
                findNode(JsonOperations.explainResultsJson(query), "Bitmap Index Scan");
        Assert.assertTrue(isCorrectPlan != null);
    }

    @Test
    public void testIndexScanMaxTuples() {
        checkTestIndexScan(500);
        checkTestIndexScan(800);
        checkTestIndexScan(1000);
        checkTestIndexScan(10000);
        checkTestIndexScan(100000);
    }
}

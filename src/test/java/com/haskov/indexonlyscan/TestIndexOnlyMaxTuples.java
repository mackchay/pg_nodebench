package com.haskov.indexonlyscan;

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

public class TestIndexOnlyMaxTuples {
    private Configuration initDB(int size) {
        String argArray = "-h localhost -j testplans/indexonlyscan.json -S " + size;
        String[] args = List.of(argArray.split(" ")).toArray(new String[0]);
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        return conf;
    }

    //TODO: fix test
    private void checkTestIndexOnlyScan(int sizeOfTable) {
//        Configuration conf = initDB(sizeOfTable);
//        Node node = NodeFactory.createNode("IndexOnlyScan");
//        List<String> tables = node.prepareTables(conf.tableSize);
//        int conditionCount = 2;
//        String query = "select x from " + tables.getFirst();
//        ScanCostCalculator scanCostCalculator = new ScanCostCalculator();
//        long maxTuples = scanCostCalculator.calculateIndexOnlyScanMaxTuples(tables.getFirst(), "x",
//                        conditionCount, 0);
//        query += " where x > 0 and x < " + maxTuples;
//        V2.explain(LoggerFactory.getLogger("TestIndexOnlyScanMaxTuples"), query);
//        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query), "Index Only Scan");
//        PgJsonPlan isCorrectPlan = JsonOperations.
//                        findNode(JsonOperations.explainResultsJson(query), "Index Only Scan");
//        Assert.assertTrue(isCorrectPlan != null);
    }

    @Test
    public void testIndexOnlyScanMaxTuples() {
        checkTestIndexOnlyScan(500);
        checkTestIndexOnlyScan(10000);
        checkTestIndexOnlyScan(10000);
        checkTestIndexOnlyScan(100000);
    }
}

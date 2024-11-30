package com.haskov.seqscan;

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

import java.util.List;
import java.util.Objects;

public class TestSeqScanCost {

    private Configuration initDB(int size) {
        String argArray = "-h localhost -j testplans/seqscan.json -S " + size;
        String[] args = List.of(argArray.split(" ")).toArray(new String[0]);
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        return conf;
    }

    //TODO: fix test
    private void checkTestSeqScan(int sizeOfTable, String conditions, int conditionCount) {
//        Configuration conf = initDB(sizeOfTable);
//        Node node = NodeFactory.createNode("SeqScan");
//        List<String> tables = node.prepareTables(conf.tableSize);
//        String query = "select * from " + tables.getFirst() + conditions;
//        double actualCost = ScanCostCalculator.calculateSeqScanCost(tables.getFirst(), conditionCount);
//        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query), "Seq Scan");
//        double expectedCost = Objects.requireNonNull(JsonOperations.
//                        findNode(JsonOperations.explainResultsJson(query), "Seq Scan")).
//                getJson().get("Total Cost").getAsDouble();
//        Assert.assertEquals(expectedCost, actualCost, 0.005);
    }

    @Test
    public void testSeqScan() {
        checkTestSeqScan(500, "", 0);
        checkTestSeqScan(1000, " where x > 0", 1);
        checkTestSeqScan(10000, " where x > 0 and x < 5000", 2);
        checkTestSeqScan(100000, " where x > 0 and x < 5000 and y > 1000 and y < 8000", 4);
    }
}

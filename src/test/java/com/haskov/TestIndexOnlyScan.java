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

public class TestIndexOnlyScan {

    private Configuration initDB(int size) {
        String argArray = "-h localhost -n IndexOnlyScan -S " + size;
        String[] args = List.of(argArray.split(" ")).toArray(new String[0]);
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        return conf;
    }

    private void checkTestSeqScan(int sizeOfTable, String conditions, int conditionCount, double selectivity) {
        Configuration conf = initDB(sizeOfTable);
        Node node = NodeFactory.createNode(conf.node);
        List<String> tables = node.prepareTables(conf.sizeOfTable);
        String query = "select * from " + tables.getFirst() + conditions;
        double actualCost = ScanCostCalculator.calculateIndexOnlyScanCost(tables.getFirst(), "x",
                conditionCount, conditionCount, selectivity);
        PgJsonPlan plan = JsonOperations.findNode(JsonOperations.explainResultsJson(query), "Index Only Scan");
        double expectedCost = Objects.requireNonNull(JsonOperations.
                        findNode(JsonOperations.explainResultsJson(query), "Index Only Scan")).
                getJson().get("Total Cost").getAsDouble();
        V2.explain(LoggerFactory.getLogger("TestIndexOnlyScan"), query);
        Assert.assertEquals(expectedCost, actualCost, expectedCost * 0.1);
    }

    @Test
    public void testSeqScan() {
        checkTestSeqScan(500, " where x < 2", 1, (double) 1 / 500);
        checkTestSeqScan(10000, " where x > 0 and x < 501", 2, (double) 500 /10000);
        checkTestSeqScan(10000, " where x < 5001", 1, (double) 5000 /10000);
        checkTestSeqScan(100000, " where x < 40001", 1, (double) 40000 /100000);
        checkTestSeqScan(100000, " where x > 0 and x < 500 and y > 0 and y < 500", 4,
                (double) 499 /100000);
        checkTestSeqScan(100000, " where x > 0 and x < 50001 and y > 0 and y < 50001", 4,
                (double) 50000/100000);
        checkTestSeqScan(500, " where x > 0 and x < 100 and y > 0 and y < 100", 4, (double) 100 / 500);
    }
}

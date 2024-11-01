package com.haskov;

import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.json.JsonOperations;
import com.haskov.json.JsonPlan;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeFactory;
import com.haskov.test.TestUtils;

import java.util.List;

import static com.haskov.json.JsonOperations.explainResultsJson;
import static com.haskov.json.JsonOperations.findNode;

public class Main {
    public static void main(String[] args) {
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        Node node = NodeFactory.createNode(conf.node);
        List<String> tableNames = node.prepareTables(conf.sizeOfTable);
        String query = node.buildQuery(tableNames);
        //TestUtils.testQueriesOnNode(new String[]{query}, conf.node);
        for (int i = 0; i < 2000; i++) {
            query = node.buildQuery(tableNames);
            JsonPlan plan = findNode(explainResultsJson(query), String.join(" ", conf.node.split("(?=[A-Z])")));
            if (plan == null) {
                System.out.println(query);
                TestUtils.testQueriesOnNode(new String[]{query}, conf.node);
                throw new RuntimeException();
            }
        }
    }
}

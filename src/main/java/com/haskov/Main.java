package com.haskov;

import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.json.JsonPlan;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeFactory;
import com.haskov.test.TestUtils;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        Node node = NodeFactory.createNode(conf.node);
        long loadSize = 100000;
        String[] queries = LoadGenerator.generate(List.of(node),
                conf.sizeOfTable, loadSize).toArray(new String[0]);
        TestUtils.testQueriesOnNode(queries, conf.node);
    }
}

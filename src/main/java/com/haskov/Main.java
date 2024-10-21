package com.haskov;

import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeFactory;
import com.haskov.test.TestUtils;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        Node node = NodeFactory.createNode(conf.node);
        List<String> tableNames = node.prepareTables(conf.sizeOfTable);
        String query = node.buildQuery(tableNames);
        TestUtils.testQueriesOnNode(new String[]{query}, conf.node);
    }
}

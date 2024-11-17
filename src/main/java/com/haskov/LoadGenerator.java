package com.haskov;

import com.haskov.nodes.Node;

import java.util.*;

public class LoadGenerator {

    public static List<String> generate(List<Node> nodes, long tableSize, long loadSize) {
        Random r = new Random();
        List<String> result = new ArrayList<>();
        Map<Node, List<String>> tables = new HashMap<>();
        for (Node node : nodes) {
            tables.put(node, node.prepareTables(tableSize));
        }
        for (int i = 0; i < loadSize; i++) {
            int randInt = r.nextInt(nodes.size());
            Node node = nodes.get(randInt);
            result.add(node.buildQuery(tables.get(node)));
        }
        return result;
    }
}

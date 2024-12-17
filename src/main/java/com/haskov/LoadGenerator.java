package com.haskov;

import com.haskov.nodes.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LoadGenerator {

    public static List<String> generate(List<Node> nodes, long tableSize, long loadSize) {
        Random r = new Random();
        List<String> result = new ArrayList<>();
        Map<Node, List<String>> tables = new HashMap<>();
        Logger logger = LoggerFactory.getLogger(LoadGenerator.class);
        for (Node node : nodes) {
            //tables.put(node, node.prepareTables(tableSize));
        }

        for (int i = 0; i < loadSize; i++) {
            int randInt = r.nextInt(nodes.size());
            Node node = nodes.get(randInt);
            String query = node.buildQuery();
            result.add(query);
            logger.info(query);
        }
        return result;
    }
}

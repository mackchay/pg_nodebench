package com.haskov.nodes;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class NodeFactory {
    private static final Map<String, Class<? extends Node>> nodeMap = new HashMap<>();

    static {
        nodeMap.put("SeqScan", SeqScan.class);
        //nodeMap.put("IndexScan", IndexScan.class);
        nodeMap.put("IndexOnlyScan", IndexOnlyScan.class);
    }

    // Метод для создания узла
    public static Node createNode(String nodeType) {
        Class<? extends Node> nodeClass = nodeMap.get(nodeType);

        if (nodeClass == null) {
            throw new IllegalArgumentException("Unknown node type: " + nodeType);
        }

        try {
            return nodeClass.getDeclaredConstructor().newInstance(); // Создаем объект через рефлексию
        } catch (Exception e) {
            throw new RuntimeException("Error creating node instance", e);
        }
    }
}

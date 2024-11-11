package com.haskov.nodes;

import com.haskov.nodes.joins.NestedLoop;
import com.haskov.nodes.scans.BitmapIndexScan;
import com.haskov.nodes.scans.IndexOnlyScan;
import com.haskov.nodes.scans.IndexScan;
import com.haskov.nodes.scans.SeqScan;

import java.util.HashMap;
import java.util.Map;

public class NodeFactory {
    private static final Map<String, Class<? extends Node>> nodeMap = new HashMap<>();

    static {
        nodeMap.put("SeqScan", SeqScan.class);
        nodeMap.put("IndexScan", IndexScan.class);
        nodeMap.put("IndexOnlyScan", IndexOnlyScan.class);
        nodeMap.put("BitmapIndexScan", BitmapIndexScan.class);
        nodeMap.put("NestedLoop", NestedLoop.class);
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

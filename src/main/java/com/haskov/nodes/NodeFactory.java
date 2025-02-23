package com.haskov.nodes;

import com.haskov.nodes.functions.*;
import com.haskov.nodes.joins.Hash;
import com.haskov.nodes.joins.HashJoin;
import com.haskov.nodes.joins.MergeJoin;
import com.haskov.nodes.joins.NestedLoop;
import com.haskov.nodes.scans.*;
import com.haskov.nodes.subquery.SubqueryScan;
import com.haskov.nodes.unions.Append;
import com.haskov.nodes.unions.SetOp;

import java.util.HashMap;
import java.util.Map;

public class NodeFactory {
    private static final Map<String, Class<? extends Node>> nodeMap = new HashMap<>();

    static {
        nodeMap.put("SeqScan", SeqScan.class);
        nodeMap.put("IndexScan", IndexScan.class);
        nodeMap.put("IndexOnlyScan", IndexOnlyScan.class);
        nodeMap.put("BitmapIndexScan", BitmapIndexScan.class);
        nodeMap.put("BitmapHeapScan", BitmapHeapScan.class);
        nodeMap.put("NestedLoop", NestedLoop.class);
        nodeMap.put("Aggregate", Aggregate.class);
        nodeMap.put("Append", Append.class);
        nodeMap.put("HashJoin", HashJoin.class);
        nodeMap.put("MergeJoin", MergeJoin.class);
        nodeMap.put("Hash", Hash.class);
        nodeMap.put("Materialize", Materialize.class);
        nodeMap.put("GroupAggregate", GroupAggregate.class);
        nodeMap.put("HashAggregate", HashAggregate.class);
        nodeMap.put("Sort", Sort.class);
        nodeMap.put("Group", Group.class);
        nodeMap.put("SetOp", SetOp.class);
        nodeMap.put("SubqueryScan", SubqueryScan.class);
        nodeMap.put("Unique", Unique.class);
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

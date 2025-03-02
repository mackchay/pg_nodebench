package com.haskov.nodes;

import com.haskov.QueryBuilder;
import com.haskov.json.JsonPlan;
import com.haskov.nodes.nonscans.NonTableScan;
import com.haskov.nodes.nonscans.Result;
import com.haskov.nodes.scans.TableScan;
import com.haskov.types.TableBuildResult;

import java.util.ArrayList;
import java.util.List;

public class NodeTree {
    private final Node parent;
    private final List<NodeTree> children;
    private long tableSize = 0;

    public NodeTree(JsonPlan plan) {
        parent = NodeFactory.createNode(plan.getNodeType());
        children = new ArrayList<>();
        parent.setParameters(plan.getParams());
        if (plan.getPlans() != null) {
            for (JsonPlan nodePlan : plan.getPlans()) {
                children.add(new NodeTree(nodePlan));
            }
        }
    }

    public List<TableBuildResult> createTables(long tableSize) {
        this.tableSize = tableSize;
        if (parent instanceof TableScan tableScan) {
            return new ArrayList<>(List.of(tableScan.initScanNode(tableSize)));
        }
        if (parent instanceof NonTableScan) {
            return new ArrayList<>();
        }
        if (children.isEmpty()) {
            throw new RuntimeException("Scan or result must be in leaf.");
        }

        List<TableBuildResult> tables = new ArrayList<>();
        for (NodeTree child : children) {
            tables.addAll(child.createTables(tableSize));
        }

        if (parent instanceof InternalNode internalNode) {
            internalNode.initInternalNode(children.stream().map(e -> e.parent).toList());
        }

        return tables;
    }

    public void prepareQuery() {
        if (parent instanceof TableScan tableScan) {
            tableScan.prepareScanQuery();
        }

        for (NodeTree child : children) {
            child.prepareQuery();
        }
    }

    public String buildQuery() {
        QueryBuilder qb = new QueryBuilder();
        qb.setMinMax(0L, tableSize - 1);
        qb.setMinMaxTuples(0L, tableSize - 1);
        return parent.buildQuery(qb).build();
    }

}

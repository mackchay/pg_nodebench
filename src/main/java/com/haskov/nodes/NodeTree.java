package com.haskov.nodes;

import com.haskov.QueryBuilder;
import com.haskov.QueryGenerator;
import com.haskov.json.JsonPlan;
import com.haskov.nodes.functions.Materialize;
import com.haskov.nodes.joins.Join;
import com.haskov.nodes.scans.Scan;
import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class NodeTree {
    private Node parent;
    private List<NodeTree> children;
    private long tableSize = 0;

    public NodeTree(JsonPlan plan) {
        parent = NodeFactory.createNode(plan.getNodeType());
        children = new ArrayList<>();
        if (plan.getPlans() != null) {
            for (JsonPlan nodePlan : plan.getPlans()) {
                children.add(new NodeTree(nodePlan));
            }
        }
    }

    public List<TableBuildResult> createTables(long tableSize) {
        this.tableSize = tableSize;
        if (parent instanceof Scan scan) {
            return new ArrayList<>(List.of(scan.initScanNode(tableSize)));
        }
        if (children.isEmpty()) {
            throw new RuntimeException("Scan or result must be in leaf.");
        }

        List<TableBuildResult> tables = new ArrayList<>();
        for (NodeTree child : children) {
            tables.addAll(child.createTables(tableSize));
        }

        return tables;
    }

    public void prepareQuery() {
        if (parent instanceof Scan scan) {
            scan.prepareScanQuery();
        }

        List<TableBuildResult> tables = new ArrayList<>();
        for (NodeTree child : children) {
            child.prepareQuery();
        }

        if (parent instanceof InternalNode internalNode) {
            internalNode.initInternalNode(children.stream().map(e -> e.parent).toList());
        }
    }

    public String buildQuery() {
        QueryBuilder qb = new QueryBuilder();
        Pair<Long, Long> tuplesRange = parent.getTuplesRange();
        qb.setTuples(tuplesRange.getLeft(), tuplesRange.getRight());
        qb.setMinMax(0L, tableSize - 1);
        return buildQuery(qb).build();
    }

    public QueryBuilder buildQuery(QueryBuilder queryBuilder) {

        if (parent instanceof Scan) {
            return parent.buildQuery(queryBuilder);
        }
        for (NodeTree child : children) {
            queryBuilder = child.buildQuery(queryBuilder);
            if (parent instanceof Join) {
                continue;
            }
            if (children.size() == 1) {
                queryBuilder = parent.buildQuery(queryBuilder);
            }
            if (children.indexOf(child) != children.size() - 1) {
                queryBuilder = parent.buildQuery(queryBuilder);
            }
        }
        if (parent instanceof Join) {
            parent.buildQuery(queryBuilder);
        }
        return queryBuilder;
    }

}

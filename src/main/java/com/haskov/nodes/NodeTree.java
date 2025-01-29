package com.haskov.nodes;

import com.haskov.QueryBuilder;
import com.haskov.json.JsonPlan;
import com.haskov.nodes.functions.Materialize;
import com.haskov.nodes.joins.HashJoin;
import com.haskov.nodes.joins.Join;
import com.haskov.nodes.joins.MergeJoin;
import com.haskov.nodes.scans.Scan;
import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class NodeTree {
    private Node parent;
    private List<NodeTree> children;
    private NodeTreeData nodeTreeData;

    public NodeTree(JsonPlan plan) {
        parent = NodeFactory.createNode(plan.getNodeType());
        children = new ArrayList<>();
        nodeTreeData = new NodeTreeData(
                0,
                0,
                0,
                Integer.MAX_VALUE,
                0,
                0,
                1,
                false
        );
        if (plan.getPlans() != null) {
            for (JsonPlan nodePlan : plan.getPlans()) {
                children.add(new NodeTree(nodePlan));
            }
        }
    }

    public List<TableBuildResult> createTables(long tableSize) {
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
        parent.initNode(tables.stream().map(TableBuildResult::tableName).toList());
        return tables;
    }

    public void prepareQuery() {
        setCosts();
        setTuples(nodeTreeData.getMinTuples(), nodeTreeData.getMaxTuples(), false);
    }

    //TODO prepare query, costs and tuples
    public void setCosts() {
        parent.prepareQuery();
        if (parent instanceof Scan scan) {
            Pair<Double, Double> costs = scan.getCosts();
            nodeTreeData.setStartUpCost(costs.getLeft());
            nodeTreeData.setTotalCost(costs.getRight());

            Pair<Integer, Integer> conditions = scan.getConditions();
            nodeTreeData.setIndexConditions(conditions.getLeft());
            nodeTreeData.setNonIndexConditions(conditions.getRight());
            nodeTreeData.setSel(scan.getSel());
        }

        for (NodeTree child : children) {
            child.setCosts();
            nodeTreeData.setStartUpCost(child.nodeTreeData.getStartUpCost());
            nodeTreeData.setTotalCost(child.nodeTreeData.getTotalCost());
            nodeTreeData.setIndexConditions(child.nodeTreeData.getIndexConditions());
            nodeTreeData.setNonIndexConditions(child.nodeTreeData.getNonIndexConditions());
            nodeTreeData.setSel(
                    nodeTreeData.getSel() * child.nodeTreeData.getSel()
            );
            if (child.nodeTreeData.isMaterialized()) {
                nodeTreeData.setMaterialized(true);
            }
        }

        if (parent instanceof Materialize) {
            nodeTreeData.setMaterialized(true);
        }

        if (parent instanceof Join join) {
            join.prepareJoinQuery(
                    children.getFirst().nodeTreeData,
                    children.get(1).nodeTreeData
            );
        }
    }

    public void setTuples(long newMinTuples, long newMaxTuples, boolean isRecalculate) {
        nodeTreeData.setMinTuples(newMinTuples);
        nodeTreeData.setMaxTuples(newMaxTuples);

        if (parent instanceof Join join) {
            Pair<Long, Long> tupleRange = join.getTuplesRange();
            if (tupleRange.getLeft() > nodeTreeData.getMinTuples()) {
                nodeTreeData.setMinTuples(tupleRange.getLeft());
            }
            if (tupleRange.getRight() < nodeTreeData.getMaxTuples()) {
                nodeTreeData.setMaxTuples(tupleRange.getRight());
            }
            if (join instanceof HashJoin || join instanceof MergeJoin) {
                isRecalculate = true;
            }
        }

        if (parent instanceof Scan scan) {
            Pair<Long, Long> tupleRange = scan.getTuplesRange();
            if (tupleRange.getLeft() > nodeTreeData.getMinTuples()) {
                nodeTreeData.setMinTuples(tupleRange.getLeft());
            }
            if (tupleRange.getRight() < nodeTreeData.getMaxTuples()) {
                nodeTreeData.setMaxTuples(tupleRange.getRight());
            }
            if (isRecalculate) {
                nodeTreeData.setMinTuples(scan.reCalculateMinTuple(nodeTreeData.getMinTuples()));
            }
        }
        for (NodeTree child : children) {
            child.setTuples(nodeTreeData.getMinTuples(), nodeTreeData.getMaxTuples(), isRecalculate);
        }
    }

//    private Pair<Double, Double> setCosts() {
//        if (parent instanceof Scan scan) {
//            Pair<Double, Double> costs = scan.getCosts();
//            startUpCost += costs.getLeft();
//            totalCost += costs.getRight();
//            return new ImmutablePair<>(startUpCost, totalCost);
//        }
//        if (parent instanceof Join join) {
//            Pair<Double, Double> costs = join.getCosts();
//        }
//        for (NodeTree child : children) {
//
//        }
//    }

    public QueryBuilder buildQuery(QueryBuilder queryBuilder) {
        queryBuilder.setTuples(nodeTreeData.getMinTuples(), nodeTreeData.getMaxTuples());
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

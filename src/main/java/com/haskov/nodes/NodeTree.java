package com.haskov.nodes;

import com.google.gson.JsonElement;
import com.haskov.QueryBuilder;
import com.haskov.json.JsonPlan;
import com.haskov.json.PgJsonPlan;
import com.haskov.nodes.joins.Join;
import com.haskov.nodes.scans.Scan;
import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class NodeTree {
    private Node parent;
    private List<NodeTree> children;
    private double startUpCost;
    private double totalCost;
    private long minTuples;
    private long maxTuples;
    private int nonIndexConditions;
    private int indexConditions;
    private double sel;
    private final List<TableBuildResult> tableBuildResults = new ArrayList<>();

    public NodeTree(JsonPlan plan) {
        parent = NodeFactory.createNode(plan.getNodeType());
        children = new ArrayList<>();
        startUpCost = 0;
        totalCost = 0;
        minTuples = 0;
        maxTuples = Integer.MAX_VALUE;
        nonIndexConditions = 0;
        indexConditions = 0;
        sel = 1;
        if (plan.getPlans() != null) {
            for (JsonPlan nodePlan : plan.getPlans()) {
                children.add(new NodeTree(nodePlan));
            }
        }
    }

    public NodeTree (PgJsonPlan plan) {
        parent = NodeFactory.createNode(plan.getNodeType().replace(" ", ""));
        children = new ArrayList<>();
        startUpCost = 0;
        totalCost = 0;
        minTuples = 0;
        maxTuples = Integer.MAX_VALUE;
        if (plan.getJson().get("Plans") != null) {
            for (JsonElement elem : plan.getJson().get("Plans").getAsJsonArray()) {
                children.add(new NodeTree(new PgJsonPlan(elem.getAsJsonObject())));
            }
        }
    }

    public List<TableBuildResult> createTables(long tableSize) {
        if (parent instanceof Scan scan) {
            tableBuildResults.add((scan.initScanNode(tableSize)));
            return tableBuildResults;
        }
        if (children.isEmpty()) {
            throw new RuntimeException("Scan or result must be in leaf.");
        }
        for (NodeTree child : children) {
            tableBuildResults.addAll(child.createTables(tableSize));
        }
        parent.initNode(tableBuildResults.stream().map(TableBuildResult::tableName).toList());
        return tableBuildResults;
    }

    public void prepareQuery() {
        setCosts();
        setTuples(minTuples, maxTuples, false);
    }

    //TODO prepare query, costs and tuples
    public void setCosts() {
        if (parent instanceof Scan scan) {
            parent.prepareQuery();
            Pair<Double, Double> costs = scan.getCosts();
            startUpCost = costs.getLeft();
            totalCost = costs.getRight();

            Pair<Integer, Integer> conditions = scan.getConditions();
            indexConditions = conditions.getLeft();
            nonIndexConditions = conditions.getRight();
            sel = scan.getSel();

        }
        for (NodeTree child : children) {
            child.setCosts();
            startUpCost = child.startUpCost;
            totalCost = child.totalCost;
            indexConditions = child.indexConditions;
            nonIndexConditions = child.nonIndexConditions;
            sel *= child.sel;
        }

        parent.prepareQuery();
        if (parent instanceof Join join) {
            join.prepareJoinQuery(
                    children.getFirst().totalCost,
                    children.get(1).totalCost,
                    children.getFirst().sel,
                    children.get(1).sel,
                    children.getFirst().nonIndexConditions + children.getFirst().indexConditions,
                    children.get(1).nonIndexConditions + children.get(1).indexConditions,
                    children.getFirst().startUpCost
            );
        }
    }

    public void setTuples(long newMinTuples, long newMaxTuples, boolean isRecalculate) {
        minTuples = newMinTuples;
        maxTuples = newMaxTuples;
        if (parent instanceof Join join) {
            Pair<Long, Long> tupleRange = join.getTuplesRange();
            if (tupleRange.getLeft() > minTuples) {
                minTuples = tupleRange.getLeft();
            }
            if (tupleRange.getRight() < maxTuples) {
                maxTuples = tupleRange.getRight();
            }
            isRecalculate = true;
        }

        if (parent instanceof Scan scan) {
            Pair<Long, Long> tupleRange = scan.getTuplesRange();
            if (tupleRange.getLeft() > minTuples) {
                minTuples = tupleRange.getLeft();
            }
            if (tupleRange.getRight() < maxTuples) {
                maxTuples = tupleRange.getRight();
            }
            if (isRecalculate) {
                minTuples = scan.reCalculateMinTuple(minTuples);
            }
        }
        for (NodeTree child : children) {
            child.setTuples(minTuples, maxTuples, isRecalculate);
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
        queryBuilder.setTuples(minTuples, maxTuples);
        if (parent instanceof Scan) {
            return parent.buildQuery(queryBuilder);
        }
        for (NodeTree child : children) {
            queryBuilder = child.buildQuery(queryBuilder);
            if (children.size() == 1) {
                queryBuilder = parent.buildQuery(queryBuilder);
            }
            if (children.indexOf(child) != children.size() - 1) {
                queryBuilder = parent.buildQuery(queryBuilder);
            }
        }
        return queryBuilder;
    }

    private void setTuplesAndCosts(NodeTree nodeTree) {
        Pair<Long, Long> range;
        Pair<Double, Double> costs;
        if (parent instanceof Scan scan) {
            range = scan.getTuplesRange();
            costs = scan.getCosts();
        } else if (parent instanceof Join join) {
            range = join.getTuplesRange();
            costs = join.getCosts();
        } else {
            range = new ImmutablePair<>(0L, 0L);
            costs = new ImmutablePair<>(0.0, 0.0);
        }

        if (range.getLeft() > minTuples) {
            minTuples = range.getLeft();
        }
        if (range.getRight() < maxTuples) {
            maxTuples = range.getRight();
        }
        if (children.contains(nodeTree)) {
            startUpCost = nodeTree.startUpCost + costs.getLeft();
            totalCost = nodeTree.totalCost + costs.getRight();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true; // Сравнение по ссылке
        if (o == null || getClass() != o.getClass()) return false; // Проверка на совместимость типов
        NodeTree nodeTree = (NodeTree) o;
        return Double.compare(nodeTree.startUpCost, startUpCost) == 0 &&
                Double.compare(nodeTree.totalCost, totalCost) == 0 &&
                minTuples == nodeTree.minTuples &&
                maxTuples == nodeTree.maxTuples &&
                parent.equals(nodeTree.parent) &&
                children.equals(nodeTree.children)
                && tableBuildResults.equals(nodeTree.tableBuildResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parent, children, startUpCost, totalCost, minTuples, maxTuples, tableBuildResults);
    }
}

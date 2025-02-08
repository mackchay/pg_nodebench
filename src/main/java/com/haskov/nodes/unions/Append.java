package com.haskov.nodes.unions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.InternalNode;
import com.haskov.nodes.Node;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class Append implements InternalNode {
    private List<Node> children;

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        Pair<Long, Long> minMaxTuples = qb.getMinMaxTuples();
        for (Node child : children.subList(0, children.size() - 1)) {
            qb = child.buildQuery(qb);
            qb = buildQueryHelper(qb);
            qb.setMinMaxTuples(minMaxTuples.getLeft(), minMaxTuples.getRight());
            qb.setMinMax(minMaxTuples.getLeft(), minMaxTuples.getRight());
        }

        qb = children.getLast().buildQuery(qb);
        qb.setMinMaxTuples(minMaxTuples.getLeft(), minMaxTuples.getRight());
        qb.setMinMax(minMaxTuples.getLeft(), minMaxTuples.getRight());
        return qb;
    }

    private QueryBuilder buildQueryHelper(QueryBuilder qb) {
        QueryBuilder result = new QueryBuilder();
        result.unionAll(qb);
        return result;
    }

    @Override
    public Pair<Double, Double> getCosts(double sel) {
        Pair<Double, Double> costs;
        double startUpCost = 0.0;
        double totalCost = 0.0;
        for (Node n : children) {
            costs = n.getCosts(sel);
            startUpCost += costs.getLeft();
            totalCost += costs.getRight();
        }
        return new ImmutablePair<>(startUpCost, totalCost);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        return new ImmutablePair<>(0L, Long.MAX_VALUE);
    }

    @Override
    public List<String> getTables() {
        List<String> tables = new ArrayList<>();
        for (Node n : children) {
            tables.addAll(n.getTables());
        }
        return tables;
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        Integer conditions = 0, indexConditions = 0;
        for (Node n : children) {
            Pair<Integer, Integer> tmpConditions = n.getConditions();
            conditions += tmpConditions.getRight();
            indexConditions += tmpConditions.getLeft();
        }
        return new ImmutablePair<>(indexConditions, conditions);
    }

    @Override
    public void initInternalNode(List<Node> nodes) {
        children = nodes;
    }
}

package com.haskov.nodes.functions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.InternalNode;
import com.haskov.nodes.Node;
import com.haskov.types.AggregateParams;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Aggregate implements InternalNode {
    private Node child;
    private String strategy = "Default";

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        qb = child.buildQuery(qb);
        if (qb.IsSelectColumnsEmpty()) {
            throw new RuntimeException("Aggregate requires a select columns: requires Scan or Result.");
        }

        List<String> columns = new ArrayList<>(qb.getSelectColumns());
        for (String column : columns) {
            if (strategy.equals("Sorted")) {
                qb.count(column, AggregateParams.ADD);
                qb.groupBy(column);
            } else {
                qb.count(column, AggregateParams.REPLACE);
            }
        }

        if (strategy.equals("Hashed")) {
            qb.select(columns.getFirst());
            qb.groupBy(columns.getFirst());
        }
        return qb;
    }

    @Override
    public Pair<Double, Double> getCosts(double sel) {
        return child.getCosts(sel);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        return child.getTuplesRange();
    }

    @Override
    public List<String> getTables() {
        return child.getTables();
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return child.getConditions();
    }

    @Override
    public void initInternalNode(List<Node> nodes) {
        child = nodes.getFirst();
    }

    @Override
    public void setParameters(Map<String, String> params) {
        String strategy = params.get("Strategy");
        if (strategy != null) {
            this.strategy = strategy;
        }
    }
}

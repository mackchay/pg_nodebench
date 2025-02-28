package com.haskov.nodes.functions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.InternalNode;
import com.haskov.nodes.Node;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class Sort implements InternalNode {
    private Node child;
    private String table;
    private List<String> nonIndexedColumnsCache = new ArrayList<>();

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        qb = child.buildQuery(qb);
        if (qb.IsSelectColumnsEmpty()) {
            throw new RuntimeException("Sort requires a select columns: requires Scan or Result.");
        }

        List<String> columns = new ArrayList<>(qb.getSelectColumns());
        for (String column : columns) {
            if (nonIndexedColumnsCache.contains(column)) {
                qb.orderBy(column);
                continue;
            }
            if (!column.contains("NULL::INT") && !column.contains("dummy")) {
                if (!SQLUtils.hasIndexOnColumn(table, column.split("\\.")[1])) {
                    qb.orderBy(column);
                    nonIndexedColumnsCache.add(column);
                }
            }
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
        return List.of(table);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return child.getConditions();
    }

    @Override
    public void initInternalNode(List<Node> nodes) {
        child = nodes.getLast();
        table = child.getTables().getLast();
    }
}

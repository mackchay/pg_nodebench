package com.haskov.nodes.functions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.InternalNode;
import com.haskov.nodes.Node;
import com.haskov.types.ReplaceOrAdd;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.haskov.bench.V2.getColumnsAndTypes;

public class HashAggregate implements InternalNode {
    private Node child;
    private List<String> tables;
    private List<String> columns = new ArrayList<>();

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        qb = child.buildQuery(qb);
        if (qb.IsSelectColumnsEmpty()) {
            throw new RuntimeException("Aggregate requires a select columns: requires Scan or Result.");
        }

        columns = qb.getAllSelectColumns();
        for (String column : columns) {
            qb.count(column, ReplaceOrAdd.REPLACE);
        }
        qb.select("NULL::INT");
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
        return tables;
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return child.getConditions();
    }

    @Override
    public void initInternalNode(List<Node> nodes) {
        child = nodes.getLast();
        tables = child.getTables();
        for (String table : tables) {
            columns.addAll(getColumnsAndTypes(table).keySet());
        }
    }
}

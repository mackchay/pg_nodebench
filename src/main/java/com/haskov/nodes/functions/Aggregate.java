package com.haskov.nodes.functions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.InternalNode;
import com.haskov.nodes.Node;
import com.haskov.types.ReplaceOrAdd;
import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

import static com.haskov.bench.V2.getColumnsAndTypes;

public class Aggregate implements InternalNode {
    private Node child;
    private String table;
    private final List<String> columns = new ArrayList<>();

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        if (!qb.hasSelectColumns()) {
            throw new RuntimeException("Aggregate requires a select columns: requires Scan or Result.");
        }

        for (String column : columns) {
            qb.count(table, column, ReplaceOrAdd.REPLACE);
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
        child = nodes.getFirst();
        table = child.getTables().getFirst();
        columns.addAll(getColumnsAndTypes(table).keySet());
    }
}

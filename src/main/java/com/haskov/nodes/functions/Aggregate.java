package com.haskov.nodes.functions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.Node;
import com.haskov.types.ReplaceOrAdd;
import com.haskov.types.TableBuildResult;

import java.util.ArrayList;
import java.util.List;

import static com.haskov.bench.V2.getColumnsAndTypes;

public class Aggregate implements Node {
    private String table;
    private final List<String> columns = new ArrayList<>();

    @Override
    public void initNode(List<String> tables) {
        table = tables.getFirst();
        columns.addAll(getColumnsAndTypes(table).keySet());
    }

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
    public String buildQuery() {
        return "";
    }
}

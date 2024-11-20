package com.haskov.nodes.functions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.Node;
import com.haskov.types.ReplaceOrAdd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.haskov.bench.V2.getColumnsAndTypes;

public class Aggregate implements Node {

    @Override
    public String buildQuery(List<String> tables) {
        QueryBuilder qb = new QueryBuilder();
        return "";
    }

    @Override
    public QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        if (!qb.hasSelectColumns()) {
            throw new RuntimeException("Aggregate requires a select columns: requires Scan or Result.");
        }
        String table = tables.getFirst();
        List<String> columnsAndTypes = new ArrayList<>(getColumnsAndTypes(table).keySet());
        for (String column : columnsAndTypes) {
            qb.count(table, column, ReplaceOrAdd.REPLACE);
        }
        return qb;
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        return List.of();
    }
}

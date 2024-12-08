package com.haskov.nodes.functions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.Node;

import java.util.List;

public class Materialize implements Node {
    @Override
    public String buildQuery(List<String> tables) {
        return "";
    }

    @Override
    public QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        return Node.super.buildQuery(tables, qb);
    }
}

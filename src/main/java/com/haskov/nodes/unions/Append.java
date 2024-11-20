package com.haskov.nodes.unions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.Node;

import java.util.List;

public class Append implements Node {

    @Override
    public String buildQuery(List<String> tables) {
        return "";
    }

    @Override
    public QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        QueryBuilder q = new QueryBuilder();
        q.unionAll(qb);
        return q;
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        return List.of();
    }
}

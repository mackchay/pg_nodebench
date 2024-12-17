package com.haskov.nodes.unions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.Node;
import com.haskov.types.TableBuildResult;

import java.util.List;

public class Append implements Node {

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        QueryBuilder q = new QueryBuilder();
        q.unionAll(qb);
        return q;
    }

    @Override
    public String buildQuery() {
        return "";
    }
}

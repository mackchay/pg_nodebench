package com.haskov.nodes;

import com.haskov.QueryBuilder;
import com.haskov.types.TableBuildResult;

import java.util.List;

public interface Node {

    public default void initNode(List<String> tables) {

    }

    public String buildQuery();

    public default QueryBuilder buildQuery(QueryBuilder qb) {
        return qb;
    }
}

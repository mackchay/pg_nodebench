package com.haskov.nodes;

import com.haskov.QueryBuilder;
import com.haskov.types.TableBuildResult;

import java.util.List;

public interface Node {

    public String buildQuery(List<String> tables);

    public default QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        return qb;
    }


    public TableBuildResult prepareTables(Long tableSize);
}

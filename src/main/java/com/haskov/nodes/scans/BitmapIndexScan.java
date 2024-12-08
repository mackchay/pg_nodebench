package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.nodes.Node;
import com.haskov.types.InsertType;
import com.haskov.types.TableBuildResult;

import java.util.List;

import static com.haskov.tables.TableBuilder.buildRandomTable;


public class BitmapIndexScan implements Node, Scan {
    @Override
    public String buildQuery(List<String> tables) {
        return "";
    }

    @Override
    public QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        qb.from(tables.getFirst());
        return qb;
    }

    @Override
    public TableBuildResult createTable(Long tableSize) {
        String tableName = "pg_bitmapscan";
        return buildRandomTable(tableName, tableSize, InsertType.RANDOM);
    }
}

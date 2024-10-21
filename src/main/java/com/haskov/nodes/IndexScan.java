package com.haskov.nodes;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.tables.DropTable;
import com.haskov.utils.SQLUtils;

import java.util.*;

public class IndexScan implements Node {

    //TODO fix IndexScan
    @Override
    public String buildQuery(List<String> tables) {
        QueryBuilder qb = new QueryBuilder();
        return qb.build();
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        String tableName = "pg_indexscan";
        DropTable.dropTable(tableName);
        V2.sql("create table " + tableName + " ( x integer, y integer )");
        V2.sql("insert into " + tableName + " (x, y) select generate_series(1, ?), floor(random() * ?) + 1",
                tableSize, tableSize);
        V2.sql("create index if not exists pg_indexscan_idx on + " + tableName + " (x)");
        return List.of(tableName);
    }
}

package com.haskov.nodes;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.tables.DropTable;

import java.util.*;

import static com.haskov.bench.V2.getColumnsAndTypes;

public class SeqScan implements Node{

    @Override
    public String buildQuery(List<String> tables) {
        QueryBuilder qb = new QueryBuilder();
        Random random = new Random();
        int tableCount = random.nextInt(tables.size()) + 1;
        Collections.shuffle(tables);

        for (int i = 0; i < tableCount; i++) {
            List<String> columns = new ArrayList<>(getColumnsAndTypes(tables.get(i)).keySet());
            int columnsCount = random.nextInt(columns.size()) + 1;
            Collections.shuffle(columns);
            qb.from(tables.get(i));
            for (int j = 0; j < columnsCount; j++) {
                qb.select(tables.get(i) + "." + columns.get(j));
            }
        }

        return qb.build();
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        String tableName = "pg_seqscan";
        DropTable.dropTable(tableName);
        V2.sql("create table " + tableName + " ( x integer)");
        V2.sql("insert into " + tableName + " (x) select generate_series(1, ?)",
                tableSize);
        V2.sql("analyze " + tableName);
        return List.of(tableName);
    }
}

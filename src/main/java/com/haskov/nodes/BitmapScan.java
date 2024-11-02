package com.haskov.nodes;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.tables.DropTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.haskov.bench.V2.getColumnsAndTypes;
import static com.haskov.bench.V2.sql;

public class BitmapScan implements Node {

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
                qb.addRandomWhere(tables.get(i), columns.get(j), this.getClass().getSimpleName());
            }
        }

        return qb.build();
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        String tableName = "pg_bitmapscan";
        DropTable.dropTable(tableName);
        sql("create table " + tableName + " (x integer, y integer)");
        sql("insert into " + tableName + " (x) select generate_series(1, ?)"
                , tableSize);
        sql("alter table " + tableName + " add column y " + tableName + "int4 default random() * ?", tableSize);
        sql("CREATE TEMP TABLE temp_random_numbers AS " +
                "SELECT generate_series(1, ?) AS num " +
                "ORDER BY RANDOM()", tableSize);
        sql("UPDATE " + tableName +
                " SET y = temp.num " +
                "FROM temp_random_numbers AS temp " +
                "WHERE " + tableName + ".x = ? - temp.num + 1", tableSize);
        
        sql("create index if not exists pg_bitmapscan_idx on " + tableName + "(x, y)");
        V2.sql("vacuum freeze analyze " + tableName);
        return List.of(tableName);
    }
}

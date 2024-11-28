package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.nodes.Node;
import com.haskov.tables.DropTable;
import com.haskov.utils.SQLUtils;

import java.util.*;

import static com.haskov.bench.V2.getColumnsAndTypes;

@Scan
public class SeqScan implements Node {

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
                if (random.nextBoolean()) {
                    qb.addRandomWhere(tables.get(i), columns.get(j), this.getClass().getSimpleName());
                } else {
                    qb.select(tables.get(i) + "." + columns.get(j));
                }
            }
        }

        return qb.build();
    }

    @Override
    public QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        Random random = new Random();
        int tableCount = random.nextInt(tables.size()) + 1;
        Collections.shuffle(tables);

        for (int i = 0; i < tableCount; i++) {
            List<String> columns = new ArrayList<>(getColumnsAndTypes(tables.get(i)).keySet());
            int columnsCount = random.nextInt(columns.size()) + 1;
            Collections.shuffle(columns);
            qb.from(tables.get(i));
            for (int j = 0; j < columnsCount; j++) {
                if (random.nextBoolean()) {
                    qb.addRandomWhere(tables.get(i), columns.get(j), this.getClass().getSimpleName());
                } else {
                    qb.select(tables.get(i) + "." + columns.get(j));
                }
            }
        }
        return qb;
    }

    public List<String> prepareTables(Long tableSize) {
        String tableName = "pg_seqscan";
        if (SQLUtils.getTableRowCount(tableName).equals(tableSize)) {
            V2.sql("vacuum freeze analyze " + tableName);
            return new ArrayList<>(List.of(tableName));
        }
        DropTable.dropTable(tableName);
        V2.sql("create table " + tableName + " ( x integer, y integer, z integer)");
        V2.sql("insert into " + tableName + " (x, y, z) select generate_series(1, ?), generate_series(1, ?)," +
                        " generate_series(1, ?)",
                tableSize, tableSize, tableSize);
        V2.sql("vacuum freeze analyze " + tableName);
        return new ArrayList<>(List.of(tableName));
    }
}

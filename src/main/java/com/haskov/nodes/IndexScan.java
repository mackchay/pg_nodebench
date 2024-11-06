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
        Random random = new Random();
        int tableCount = random.nextInt(tables.size()) + 1;
        Collections.shuffle(tables);

        List<String> indexedColumns = new ArrayList<>();
        List<String> nonIndexedColumns = new ArrayList<>();
        for (int i = 0; i < tableCount; i++) {
            Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(tables.get(i));
            Collections.shuffle(Arrays.asList(columnsAndTypes.keySet().toArray()));
            for (String column : columnsAndTypes.keySet()) {
                if (SQLUtils.hasIndexOnColumn(tables.get(i), column)) {
                    indexedColumns.add(column);
                }
                else {
                    nonIndexedColumns.add(column);
                }
            }
            qb.from(tables.get(i));
            int indexedColumnIndex = random.nextInt(indexedColumns.size()) + 1;
            int nonIndexedColumnIndex = random.nextInt(nonIndexedColumns.size()) + 1;

            qb.setIndexConditionCount((indexedColumnIndex)*2);
            qb.setConditionCount((nonIndexedColumnIndex)*2);

            for (int j = 0; j < indexedColumnIndex; j++) {
                qb.addRandomWhere(tables.get(i), indexedColumns.get(j), this.getClass().getSimpleName());
            }
            for (int j = 0; j < nonIndexedColumnIndex; j++) {
                qb.addRandomWhere(tables.get(i), nonIndexedColumns.get(j));
            }
        }

        return qb.build();
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        String tableName = "pg_indexscan";
        DropTable.dropTable(tableName);
        V2.sql("create table " + tableName + " ( x integer, y integer, z integer, w integer)");
        V2.sql("insert into " + tableName + " (x, y, z, w) select generate_series(1, ?), generate_series(1, ?), " +
                        "generate_series(1, ?), generate_series(1, ?)",
                tableSize, tableSize, tableSize, tableSize);
        V2.sql("create index if not exists pg_indexscan_idz on " + tableName + " (z)");
        V2.sql("create index if not exists pg_indexscan_idx on " + tableName + " (x)");
        V2.sql("vacuum freeze analyze " + tableName);
        return List.of(tableName);
    }
}

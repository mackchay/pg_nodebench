package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.nodes.Node;
import com.haskov.tables.DropTable;
import com.haskov.utils.SQLUtils;

import java.util.*;

@Scan
public class IndexScan implements Node {

    //TODO fix IndexScan
    @Override
    public String buildQuery(List<String> tables) {
        QueryBuilder qb = new QueryBuilder();
        Random random = new Random();
        String table = tables.getFirst();

        List<String> indexedColumns = new ArrayList<>();
        List<String> nonIndexedColumns = new ArrayList<>();
        Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(table);
        List<String> columns = Arrays.asList(columnsAndTypes.keySet().toArray(new String[0]));
        Collections.shuffle(columns);
        for (String column : columns) {
            if (SQLUtils.hasIndexOnColumn(table, column)) {
                indexedColumns.add(column);
            }
            else {
                nonIndexedColumns.add(column);
            }
        }
        qb.from(table);
        int indexedColumnIndex = 1;
        int nonIndexedColumnIndex = random.nextInt(nonIndexedColumns.size()) + 1;

        qb.setIndexConditionCount((indexedColumnIndex)*2);
        qb.setConditionCount((nonIndexedColumnIndex)*2);

        for (int j = 0; j < indexedColumnIndex; j++) {
            qb.addRandomWhere(table, indexedColumns.get(j), this.getClass().getSimpleName());
        }
        for (int j = 0; j < nonIndexedColumnIndex; j++) {
            qb.addRandomWhere(table, nonIndexedColumns.get(j));
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
        V2.sql("create index if not exists pg_indexscan_idx on " + tableName + " (x)");
        V2.sql("create index if not exists pg_indexscan_idz on " + tableName + " (z)");
        V2.sql("vacuum freeze analyze " + tableName);
        return new ArrayList<>(List.of(tableName));
    }
}

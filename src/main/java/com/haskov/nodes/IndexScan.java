package com.haskov.nodes;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.tables.DropTable;
import com.haskov.utils.SQLUtils;

import java.util.*;

public class IndexScan implements Node {


    private void addRandomWhereCondition(QueryBuilder qb, String table, String column) {
        Random random = new Random();
        qb.select(table + "." + column);
        long min = Long.parseLong(SQLUtils.getMin(table, column));
        long max = Long.parseLong(SQLUtils.getMax(table, column));

        Long maxTuples = SQLUtils.calculateIndexScanMaxTuples(table, column);
        if (maxTuples <= 0) {
            return;
        }
        Long tuples = random.nextLong(0, maxTuples);
        Long radius = random.nextLong(min, max);
        qb.where(table+ "." + column + ">" + radius).
                where(table + "." + column + "<" + (radius + tuples));
    }

    //TODO fix IndexScan
    @Override
    public String buildQuery(List<String> tables) {
        QueryBuilder qb = new QueryBuilder();
        Random random = new Random();
        int tableCount = random.nextInt(tables.size()) + 1;
        Collections.shuffle(tables);

        String indexedColumn = "", nonIndexedColumn = "";
        for (int i = 0; i < tableCount; i++) {
            Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(tables.get(i));
            Collections.shuffle(Arrays.asList(columnsAndTypes.keySet().toArray()));
            for (String column : columnsAndTypes.keySet()) {
                if (SQLUtils.hasIndexOnColumn(tables.get(i), column)) {
                    indexedColumn =  column;
                }
                else {
                    nonIndexedColumn = column;
                }
            }
            qb.from(tables.get(i));
            for (String column : columnsAndTypes.keySet()) {
                if (random.nextBoolean()) {
                    qb.orderBy(column);
                }
            }

            columnsAndTypes.remove(indexedColumn);
            columnsAndTypes.remove(nonIndexedColumn);
            addRandomWhereCondition(qb, tables.get(i), indexedColumn);
            addRandomWhereCondition(qb, tables.get(i), nonIndexedColumn);

            int columnsCount = random.nextInt(0, columnsAndTypes.size() + 1);
            Collections.shuffle(Arrays.asList(columnsAndTypes.keySet().toArray()));


            for (int j = 0; j < columnsCount; j++) {
                String column = columnsAndTypes.keySet().iterator().next();
                addRandomWhereCondition(qb, tables.get(i), column);
            }
        }

        return qb.build();
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        String tableName = "pg_indexscan";
        DropTable.dropTable(tableName);
        V2.sql("create table " + tableName + " ( x integer, y integer )");
        V2.sql("insert into " + tableName + " (x, y) select generate_series(1, ?), floor(random() * ?) + 1",
                tableSize, tableSize);
        V2.sql("create index if not exists pg_indexscan_idx on " + tableName + " (x)");
        V2.sql("vacuum freeze analyze " + tableName);
        return List.of(tableName);
    }
}

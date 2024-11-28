package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.nodes.Node;
import com.haskov.tables.DropTable;
import com.haskov.utils.SQLUtils;

import java.util.*;

import static com.haskov.bench.V2.sql;

@Scan
public class BitmapScan implements Node {

    @Override
    public String buildQuery(List<String> tables) {
        QueryBuilder qb = new QueryBuilder();
        Random random = new Random();
        String table = tables.getFirst();
        Collections.shuffle(tables);

        List<String> indexedColumns = new ArrayList<>();
        List<String> nonIndexedColumns = new ArrayList<>();
        Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(table);
        Collections.shuffle(Arrays.asList(columnsAndTypes.keySet().toArray()));
        for (String column : columnsAndTypes.keySet()) {
            if (SQLUtils.hasIndexOnColumn(table, column)) {
                indexedColumns.add(column);
            }
            else {
                nonIndexedColumns.add(column);
            }
        }
        qb.from(table);

        int indexedColumnIndex = random.nextInt(indexedColumns.size()) + 1;
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
        String tableName = "pg_bitmapscan";
        if (SQLUtils.getTableRowCount(tableName).equals(tableSize)) {
            return new ArrayList<>(List.of(tableName));
        }
        DropTable.dropTable(tableName);
        List<Long> list1 = new ArrayList<>();
        for (long i = 1; i <= tableSize; i++) {
            list1.add(i);
        }
        Collections.shuffle(list1);

        sql("create table " + tableName + " (x integer, y integer, z integer)");

        StringBuilder query = new StringBuilder("insert into " + tableName + "(x, y, z) values ");
        for (int i = 0; i < list1.size(); i++) {
            query.append("(").append(list1.get(i)).append(",").append(list1.get(i)).append(",")
                    .append(list1.get(i)).append(")").append(",");
        }
        sql(query.delete(query.length() - 1, query.length()).toString());
        
        sql("create index if not exists pg_bitmapscan_idx on " + tableName + "(x)");

        V2.sql("vacuum freeze analyze " + tableName);
        return new ArrayList<>(List.of(tableName));
    }
}

package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.nodes.Node;
import com.haskov.tables.DropTable;

import java.util.*;

public class IndexOnlyScan implements Node {

    @Override
    public String buildQuery(List<String> tables) {
        QueryBuilder qb = new QueryBuilder();
        Random random = new Random();
        int tableCount = random.nextInt(tables.size()) + 1;
        Collections.shuffle(tables);

        for (int i = 0; i < tableCount; i++) {
            Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(tables.get(i));
            int columnsCount = random.nextInt(columnsAndTypes.size()) + 1;
            qb.setIndexConditionCount(columnsCount * 2);
            Collections.shuffle(Arrays.asList(columnsAndTypes.keySet().toArray()));
            qb.from(tables.get(i));
            
            // We're expecting every column is indexed.
            Iterator<String> columnIterator = columnsAndTypes.keySet().iterator();
            for (int j = 0; j < columnsCount; j++) {
                if (!columnIterator.hasNext()) {
                    columnIterator = columnsAndTypes.keySet().iterator();
                }
                String column = columnIterator.next();
                qb.addRandomWhere(tables.get(i), column, this.getClass().getSimpleName());
            }
        }

        return qb.build();
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        String tableName = "pg_indexonlyscan";
        DropTable.dropTable(tableName);
        V2.sql("create table " + tableName + " ( x integer, y integer)");
        V2.sql("insert into " + tableName + " (x, y) select generate_series(1, ?), generate_series(1, ?)",
                tableSize, tableSize);
        V2.sql("create index if not exists pg_indexonlyscan_idx on " + tableName + " (x, y)");
        V2.sql("vacuum freeze analyze " + tableName);
        return List.of(tableName);
    }
}

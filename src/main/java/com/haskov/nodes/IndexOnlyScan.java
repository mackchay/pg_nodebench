package com.haskov.nodes;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.tables.DropTable;
import com.haskov.utils.SQLUtils;

import java.util.*;

public class IndexOnlyScan implements Node{


    //TODO make dependency of selectivity.
    @Override
    public String buildQuery(List<String> tables) {
        QueryBuilder qb = new QueryBuilder();
        Random random = new Random();
        int tableCount = random.nextInt(tables.size()) + 1;
        Collections.shuffle(tables);

        for (int i = 0; i < tableCount; i++) {
            Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(tables.get(i));
            int columnsCount = random.nextInt(0, columnsAndTypes.size() + 1);
            Collections.shuffle(Arrays.asList(columnsAndTypes.keySet().toArray()));
            qb.from(tables.get(i));
            
            // We're expecting every column is indexed.
            for (int j = 0; j < columnsCount; j++) {
                String column = columnsAndTypes.keySet().iterator().next();
                qb.select(tables.get(i) + "." + column);
                //qb.randomWhere(tables.get(i), column, columnsAndTypes.get(column));
                long min = Long.parseLong(SQLUtils.getMin(tables.get(i), column));
                long max = Long.parseLong(SQLUtils.getMax(tables.get(i), column));

                Long maxTuples = SQLUtils.calculateIndexScanMaxTuples(tables.get(i), column);
                if (maxTuples <= 0) {
                    continue;
                }
                Long tuples = random.nextLong(0, maxTuples);
                Long radius = random.nextLong(min, max);
                qb.where(tables.get(i) + "." + column + ">" + radius).
                        where(tables.get(i) + "." + column + "<" + (radius + tuples));

            }
            for (String column : columnsAndTypes.keySet()) {
                if (random.nextBoolean() || columnsCount == 0) {
                    qb.orderBy(column);
                }
            }
        }

        return qb.build();
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        String tableName = "pg_indexscan";
        DropTable.dropTable(tableName);
        V2.sql("create table " + tableName + " ( x integer)");
        V2.sql("insert into " + tableName + " (x) select generate_series(1, ?)",
                tableSize);
        V2.sql("create index if not exists pg_indexscan_idx on " + tableName + " (x)");
        V2.sql("vacuum freeze analyze " + tableName);
        return List.of(tableName);
    }
}

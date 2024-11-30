package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.nodes.Node;
import com.haskov.types.InsertType;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;

import java.util.*;

import static com.haskov.tables.TableBuilder.buildRandomTable;

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
    public TableBuildResult prepareTables(Long tableSize) {
        String tableName = "pg_bitmapscan";
        return buildRandomTable(tableName, tableSize, InsertType.RANDOM);
    }
}

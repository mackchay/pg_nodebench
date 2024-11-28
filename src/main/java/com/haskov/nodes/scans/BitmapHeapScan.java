package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.nodes.Node;
import com.haskov.utils.SQLUtils;

import java.util.*;


public class BitmapHeapScan implements Node {
    @Override
    public String buildQuery(List<String> tables) {
        return "";
    }

    @Override
    public QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        Random random = new Random();
        String table = tables.getFirst();

        List<String> indexedColumns = new ArrayList<>();
        List<String> nonIndexedColumns = new ArrayList<>();
        Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(table);
        List<String> columns = new ArrayList<>(columnsAndTypes.keySet());
        Collections.shuffle(columns);
        for (String column : columns) {
            if (SQLUtils.hasIndexOnColumn(table, column)) {
                indexedColumns.add(column);
            }
            else {
                nonIndexedColumns.add(column);
            }
        }

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

        return qb;
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        return List.of();
    }
}

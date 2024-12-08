package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.nodes.Node;
import com.haskov.types.TableBuildResult;

import java.util.*;

import static com.haskov.tables.TableBuilder.buildRandomTable;
import static com.haskov.utils.SQLUtils.hasIndexOnColumn;


public class IndexOnlyScan implements Node, Scan {

    @Override
    public String buildQuery(List<String> tables) {
        return buildQuery(tables, new QueryBuilder()).build();
    }

    @Override
    public QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        Collections.shuffle(tables);
        String table = tables.getFirst();

        Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(table);
        int columnsCount = 1;
        qb.setIndexConditionCount(columnsCount * 2);

        List<String> columns = Arrays.asList(columnsAndTypes.keySet().toArray(new String[0]));
        Collections.shuffle(columns);
        qb.from(table);

        for (String column : columns) {
            if (hasIndexOnColumn(table, column)) {
                qb.addRandomWhere(table, column, this.getClass().getSimpleName());
                break;
            }
        }

        return qb;
    }

    @Override
    public TableBuildResult createTable(Long tableSize) {
        String tableName = "pg_indexonlyscan";
        return buildRandomTable(tableName, tableSize);
    }
}

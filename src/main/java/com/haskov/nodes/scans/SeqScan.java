package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.nodes.Node;
import com.haskov.types.TableBuildResult;

import java.util.*;

import static com.haskov.bench.V2.getColumnsAndTypes;
import static com.haskov.tables.TableBuilder.buildRandomTable;
import static com.haskov.utils.SQLUtils.hasIndexOnColumn;

@Scan
public class SeqScan implements Node {

    @Override
    public String buildQuery(List<String> tables) {
        return buildQuery(tables, new QueryBuilder()).build();
    }

    @Override
    public QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        Random random = new Random();
        String table = tables.getFirst();
        List<String> columns = new ArrayList<>(getColumnsAndTypes(table).keySet());
        Collections.shuffle(columns);
        qb.from(table);
        String nonIndexedColumn = columns.stream().filter(e -> !hasIndexOnColumn(table, e)).findFirst().
                orElse(null);
        if (random.nextBoolean()) {
            qb.addRandomWhere(table, nonIndexedColumn, this.getClass().getSimpleName());
        } else {
            qb.select(table + "." + nonIndexedColumn);
        }
        columns.remove(nonIndexedColumn);
        for (String column : columns) {
            double rand = random.nextDouble();
            if (rand < 0.3 && !hasIndexOnColumn(table, column)) {
                qb.addRandomWhere(table, column, this.getClass().getSimpleName());
            } else if (rand < 0.7 && !hasIndexOnColumn(table, column)) {
                qb.select(table + "." + column);
            }
        }
        return qb;
    }

    public TableBuildResult prepareTables(Long tableSize) {
        String tableName = "pg_seqscan";
        return buildRandomTable(tableName, tableSize);
    }
}

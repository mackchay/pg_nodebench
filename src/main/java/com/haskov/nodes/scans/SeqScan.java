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
        if (random.nextBoolean() && !hasIndexOnColumn(table, columns.getFirst())) {
            qb.addRandomWhere(table, columns.getFirst(), this.getClass().getSimpleName());
        } else {
            qb.select(table + "." + columns.getFirst());
        }
        columns.remove(columns.getFirst());
        for (String column : columns) {
            double rand = random.nextDouble();
            if (rand < 0.3 && !hasIndexOnColumn(table, column)) {
                qb.addRandomWhere(table, column, this.getClass().getSimpleName());
            } else if (rand < 0.7) {
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

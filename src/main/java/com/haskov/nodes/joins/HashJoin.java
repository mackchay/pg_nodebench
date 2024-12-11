package com.haskov.nodes.joins;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.nodes.Node;
import com.haskov.tables.TableBuilder;
import com.haskov.types.JoinData;
import com.haskov.types.JoinType;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class HashJoin implements Node, Join {

    @Override
    public String buildQuery(List<String> tables) {
        return "";
    }

    @Override
    public QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        Random random = new Random();

        //Expected 2 tables.
        int tableCount = 2;
        tables = tables.subList(0, tableCount);
        String leftColumn, rightColumn;
        List<String> joinColumns = new ArrayList<>();

        for (String table : tables) {
            Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(table);
            List<String> shuffledColumns = new ArrayList<>(columnsAndTypes.keySet());
            Collections.shuffle(shuffledColumns);
            for (String column : shuffledColumns) {
                if (!SQLUtils.hasIndexOnColumn(table, column)) {
                    joinColumns.add(column);
                    break;
                }
            }
        }
        qb.join(new JoinData(
                tables.getLast(),
                tables.getFirst(),
                JoinType.USUAL,
                joinColumns.getFirst(),
                joinColumns.get(1)
                )
        );
        //qb.addRandomWhere(tables.getLast(), joinColumns.getKey());
        //qb.where(tables.getFirst() + "." + joinColumns.getValue() + " < 2");

        return qb;
    }

    @Override
    public TableBuildResult prepareJoinTable(String childName, String parentTable) {
        return new TableBuildResult(
                childName,
                TableBuilder.addForeignKey(
                        childName,
                        parentTable,
                        this.getClass().getSimpleName()
                )
        );
    }
}

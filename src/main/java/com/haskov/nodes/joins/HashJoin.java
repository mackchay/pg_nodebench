package com.haskov.nodes.joins;

import com.haskov.QueryBuilder;
import com.haskov.nodes.Node;
import com.haskov.tables.TableBuilder;
import com.haskov.types.JoinData;
import com.haskov.types.JoinType;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Random;

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

        Pair<String, String> joinColumns = SQLUtils.getJoinColumns(tables.getLast(), tables.getFirst());
        qb.join(new JoinData(
                tables.getLast(),
                tables.getFirst(),
                JoinType.USUAL,
                joinColumns.getKey(),
                joinColumns.getValue()
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

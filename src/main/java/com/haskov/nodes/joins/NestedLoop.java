package com.haskov.nodes.joins;

import com.haskov.QueryBuilder;
import com.haskov.nodes.Node;
import com.haskov.tables.DropTable;
import com.haskov.tables.TableBuilder;
import com.haskov.types.JoinData;
import com.haskov.types.JoinType;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.haskov.bench.V2.getColumnsAndTypes;

public class NestedLoop implements Node, Join {


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


        qb.join(new JoinData(
                        tables.getLast(),
                        tables.getFirst(),
                        JoinType.CROSS,
                        null,
                        null
                )
        );

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

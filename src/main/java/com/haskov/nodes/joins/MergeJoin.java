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

public class MergeJoin implements Node, Join {
    private String parentTable;
    private String childTable;


    @Override
    public void initNode(List<String> tables) {
        //Expected 2 tables.
        parentTable = tables.get(1);
        childTable = tables.get(0);
    }

    @Override
    public String buildQuery() {
        return "";
    }

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        Random random = new Random();

        Pair<String, String> joinColumns = SQLUtils.getJoinColumns(parentTable, childTable);
        qb.join(new JoinData(
                        parentTable,
                        childTable,
                        JoinType.INNER,
                        joinColumns.getKey(),
                        joinColumns.getValue()
                )
        );
        //qb.orderBy(tables.get(1) + "." + tables.get(1) + "_id");

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

    @Override
    public Pair<Double, Double> getCosts() {
        return null;
    }

    @Override
    public void prepareJoinQuery(double parentTableCost, double childTableCost, double parentTableSel, double childTableSel, int innerConditionsCount, int outerConditionsCount, double startScanCost) {

    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        return null;
    }
}

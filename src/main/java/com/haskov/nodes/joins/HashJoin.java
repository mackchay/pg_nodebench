package com.haskov.nodes.joins;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.JoinCostCalculator;
import com.haskov.nodes.Node;
import com.haskov.tables.TableBuilder;
import com.haskov.types.JoinData;
import com.haskov.types.JoinType;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class HashJoin implements Node, Join {
    private String parentTable;
    private String childTable;
    private double parentScanCost;
    private double childScanCost;
    private double startUpCost;
    private int innerConditionsCount;
    private int outerConditionsCount;
    private double parentTableSel;
    private double childTableSel;

    @Override
    public void initNode(List<String> tables) {
        //Expected 2 tables.
        parentTable = tables.get(1);
        childTable = tables.getFirst();
    }


    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        String leftColumn, rightColumn;
        List<String> joinColumns = new ArrayList<>();

        List<String> tables = List.of(parentTable, childTable);
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
                parentTable,
                childTable,
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

    @Override
    public Pair<Double, Double> getCosts() {
        double cost = JoinCostCalculator.calculateHashJoinCost(
                parentTable,
                childTable,
                parentScanCost,
                childScanCost,
                parentTableSel,
                childTableSel,
                startUpCost,
                innerConditionsCount,
                outerConditionsCount
        );
        return new ImmutablePair<>(0.0, cost);
    }

    @Override
    public void prepareJoinQuery(double parentTableCost, double childTableCost,
                                 double parentTableSel, double childTableSel,
                                 int innerConditionsCount, int outerConditionsCount, double startScanCost) {
        this.parentScanCost = parentTableCost;
        this.childScanCost = childTableCost;
        this.startUpCost = startScanCost;
        this.innerConditionsCount = innerConditionsCount;
        this.outerConditionsCount = outerConditionsCount;
        this.parentTableSel = parentTableSel;
        this.childTableSel = childTableSel;
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        Pair<Long, Long> range = JoinCostCalculator.calculateHashJoinTuplesRange(
                parentTable,
                childTable,
                parentScanCost,
                childScanCost,
                startUpCost,
                innerConditionsCount,
                outerConditionsCount
        );
        return new ImmutablePair<>(range.getLeft(), range.getRight());
    }

    @Override
    public String buildQuery() {
        return "";
    }
}

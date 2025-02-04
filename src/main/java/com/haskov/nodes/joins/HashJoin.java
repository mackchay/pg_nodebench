package com.haskov.nodes.joins;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.scan.JoinCostCalculator;
import com.haskov.nodes.Node;
import com.haskov.types.JoinData;
import com.haskov.types.JoinNodeType;
import com.haskov.types.JoinType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class HashJoin implements Join {
    private Node nodeLeft;
    private Node nodeRight;

    private String leftTable;
    private String rightTable;
    private List<String> rightTableColumns;
    private List<String> leftTableColumns;

    private final JoinCostCalculator costCalculator = new JoinCostCalculator();


    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        Pair<Long, Long> tupleRange = getTuplesRange();
        qb.setMinMaxTuples(tupleRange.getLeft(), tupleRange.getRight());
        qb = nodeLeft.buildQuery(qb);

        qb.setMinMaxTuplesForce(tupleRange.getLeft(), tupleRange.getRight());
        qb = nodeRight.buildQuery(qb);

        Collections.shuffle(rightTableColumns);
        Collections.shuffle(leftTableColumns);
        List<String> selectColumns = qb.getSelectColumns();

        qb.join(new JoinData(
                rightTable,
                leftTable,
                JoinType.USUAL,
                selectColumns.getLast().substring(selectColumns.getLast().indexOf(".")).replace(".", ""),
                selectColumns.getFirst().substring(selectColumns.getFirst().indexOf(".")).replace(".", "")
                )
        );
        //qb.addRandomWhere(tables.getLast(), joinColumns.getKey());
        //qb.where(tables.getFirst() + "." + joinColumns.getValue() + " < 2");

        return qb;
    }


    @Override
    public Pair<Double, Double> getCosts(double sel) {
        Pair<Double, Double> rightCosts = getCosts(sel);
        Pair<Double, Double> leftCosts = getCosts(sel);

        String innerTable, outerTable;
        double innerScanCost, outerScanCost;
        int innerConditionsCount, outerConditionsCount;

        int leftConditionsCount = nodeLeft.getConditions().getLeft() +
                nodeLeft.getConditions().getRight();
        int rightConditionsCount = nodeRight.getConditions().getLeft() +
                nodeRight.getConditions().getRight();

        if (rightCosts.getRight() < leftCosts.getRight()) {
            innerTable = rightTable;
            outerTable = leftTable;
            innerScanCost = rightCosts.getRight();
            outerScanCost = leftCosts.getRight();
            innerConditionsCount = rightConditionsCount;
            outerConditionsCount = leftConditionsCount;
        } else {
            innerTable = leftTable;
            outerTable = rightTable;
            innerScanCost = rightCosts.getLeft();
            outerScanCost = leftCosts.getLeft();
            innerConditionsCount = rightConditionsCount;
            outerConditionsCount = leftConditionsCount;
        }
        double startUpCost = Math.max(leftCosts.getLeft(), rightCosts.getLeft());

        double totalCost = JoinCostCalculator.calculateHashJoinCost(
                innerTable,
                outerTable,
                innerScanCost,
                outerScanCost,
                sel,
                sel,
                startUpCost,
                innerConditionsCount,
                outerConditionsCount
        );
        return new ImmutablePair<>(startUpCost, totalCost);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        Pair<Long, Long> leftTuplesRange = nodeLeft.getTuplesRange();
        Pair<Long, Long> rightTuplesRange = nodeRight.getTuplesRange();

        int leftConditionsCount = nodeLeft.getConditions().getLeft() +
                nodeLeft.getConditions().getRight();
        int rightConditionsCount = nodeRight.getConditions().getLeft() +
                nodeRight.getConditions().getRight();

        long minTuples = Math.min(leftTuplesRange.getLeft(), rightTuplesRange.getLeft());
        long maxTuples = Math.max(leftTuplesRange.getRight(), rightTuplesRange.getRight());
        Pair<Long, Long> range = costCalculator.calculateTuplesRange(
                rightTable,
                leftTable,
                nodeRight::getCosts,
                nodeLeft::getCosts,
                minTuples,
                maxTuples,
                leftConditionsCount,
                rightConditionsCount,
                JoinNodeType.HASH_JOIN
        );
        return new ImmutablePair<>(range.getLeft(), range.getRight());
    }

    @Override
    public List<String> getTables() {
        return List.of(leftTable, rightTable);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        int leftConditionsCount = nodeLeft.getConditions().getLeft() +
                nodeLeft.getConditions().getRight();
        int rightConditionsCount = nodeRight.getConditions().getLeft() +
                nodeRight.getConditions().getRight();
        return new ImmutablePair<>(leftConditionsCount, rightConditionsCount);
    }

    @Override
    public void initInternalNode(List<Node> nodes) {
        nodeLeft = nodes.getFirst();
        nodeRight = nodes.getLast();

        rightTable = nodeRight.getTables().getFirst();
        leftTable = nodeLeft.getTables().getFirst();

        Map<String, String> columnsAndTypesParent = V2.getColumnsAndTypes(rightTable);
        rightTableColumns = new ArrayList<>(columnsAndTypesParent.keySet());
        Map<String, String> columnsAndTypesChild = V2.getColumnsAndTypes(leftTable);
        leftTableColumns = new ArrayList<>(columnsAndTypesChild.keySet());
    }
}

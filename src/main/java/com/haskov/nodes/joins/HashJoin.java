package com.haskov.nodes.joins;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.join.JoinTupleRangeCalculator;
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

    private JoinTupleRangeCalculator tupleRangeCalculator;


    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        Pair<Long, Long> tupleRange = getTuplesRange();
        qb.setMinMaxTuplesForce(tupleRange.getLeft(), tupleRange.getRight());
        qb = nodeRight.buildQuery(qb);

        qb.setMinMaxTuples(tupleRange.getLeft(), tupleRange.getRight());
        qb = nodeLeft.buildQuery(qb);

        Collections.shuffle(rightTableColumns);
        Collections.shuffle(leftTableColumns);
        List<String> selectColumns = qb.getSelectColumns();

        qb.join(JoinType.USUAL);
        //qb.addRandomWhere(tables.getLast(), joinColumns.getKey());
        //qb.where(tables.getFirst() + "." + joinColumns.getValue() + " < 2");

        return qb;
    }


    @Override
    public Pair<Double, Double> getCosts(double sel) {
        Pair<Double, Double> rightCosts = nodeRight.getCosts(sel);
        Pair<Double, Double> leftCosts = nodeLeft.getCosts(sel);

        int leftConditionsCount = nodeLeft.getConditions().getLeft() +
                nodeLeft.getConditions().getRight();
        int rightConditionsCount = nodeRight.getConditions().getLeft() +
                nodeRight.getConditions().getRight();
        double innerScanCost, outerScanCost;
        int innerConditionsCount, outerConditionsCount;

        double startUpCost = Math.max(leftCosts.getLeft(), rightCosts.getLeft());

        if (rightCosts.getRight() < leftCosts.getRight()) {
            innerScanCost = rightCosts.getRight();
            outerScanCost = leftCosts.getRight();
            innerConditionsCount = rightConditionsCount;
            outerConditionsCount = leftConditionsCount;
        } else {
            innerScanCost = leftCosts.getRight();
            outerScanCost = rightCosts.getLeft();
            innerConditionsCount = rightConditionsCount;
            outerConditionsCount = leftConditionsCount;
        }

        double totalCost;
        totalCost = tupleRangeCalculator.getHashJoinCalculator().calculateCost(
                innerScanCost,
                outerScanCost,
                sel,
                sel,
                innerConditionsCount,
                outerConditionsCount,
                startUpCost
        );
        return new ImmutablePair<>(startUpCost, totalCost);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        Pair<Long, Long> leftTuplesRange = nodeLeft.getTuplesRange();
        Pair<Long, Long> rightTuplesRange = nodeRight.getTuplesRange();
        int leftConditionsCount = nodeLeft.getConditions().getLeft() + nodeLeft.getConditions().getRight();
        int rightConditionsCount = nodeRight.getConditions().getLeft() + nodeRight.getConditions().getRight();

        long minTuples = Math.min(leftTuplesRange.getLeft(), rightTuplesRange.getLeft());
        long maxTuples = Math.max(leftTuplesRange.getRight(), rightTuplesRange.getRight());
        Pair<Long, Long> range = tupleRangeCalculator.calculateTuplesRange(
                minTuples,
                maxTuples,
                leftConditionsCount,
                rightConditionsCount
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

        tupleRangeCalculator = new JoinTupleRangeCalculator(leftTable, rightTable,
                nodeLeft::getCosts, nodeRight::getCosts, JoinNodeType.HASH_JOIN);
    }
}

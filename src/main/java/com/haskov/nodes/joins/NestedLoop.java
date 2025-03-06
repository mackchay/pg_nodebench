package com.haskov.nodes.joins;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.join.JoinTupleRangeCalculator;
import com.haskov.nodes.Node;
import com.haskov.nodes.functions.Materialize;
import com.haskov.types.JoinData;
import com.haskov.types.JoinNodeType;
import com.haskov.types.JoinType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NestedLoop implements Join {
    private Node nodeLeft;
    private Node nodeRight;

    private String leftTable;
    private String rightTable;
    private List<String> rightTableColumns;
    private List<String> leftTableColumns;

    private JoinTupleRangeCalculator tupleRangeCalculator;
    private JoinNodeType type;


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

        qb.join(JoinType.NON_EQUAL);
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

        if (rightCosts.getRight() > leftCosts.getRight()) {
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
        if (type.equals(JoinNodeType.NESTED_LOOP_MATERIALIZED)) {
            totalCost = tupleRangeCalculator.getNestedLoopCalculator().calculateMaterializedCost(
                    innerScanCost,
                    outerScanCost,
                    sel,
                    sel,
                    innerConditionsCount,
                    outerConditionsCount
            );
        } else {
            totalCost = tupleRangeCalculator.getNestedLoopCalculator().calculateCost(
                    innerScanCost,
                    outerScanCost,
                    sel,
                    sel,
                    innerConditionsCount,
                    outerConditionsCount
            );
        }
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
        int leftConditionsCount = nodeLeft.getConditions().getLeft() + nodeLeft.getConditions().getRight();
        int rightConditionsCount = nodeRight.getConditions().getLeft() + nodeRight.getConditions().getRight();
        return new ImmutablePair<>(leftConditionsCount, rightConditionsCount);
    }

    @Override
    public void initInternalNode(List<Node> nodes) {
        nodeLeft = nodes.getFirst();
        nodeRight = nodes.getLast();

        leftTable = nodeLeft.getTables().getFirst();
        rightTable = nodeRight.getTables().getLast();

        Map<String, String> columnsAndTypesParent = V2.getColumnsAndTypes(rightTable);
        rightTableColumns = new ArrayList<>(columnsAndTypesParent.keySet());

        Map<String, String> columnsAndTypesChild = V2.getColumnsAndTypes(leftTable);
        leftTableColumns = new ArrayList<>(columnsAndTypesChild.keySet());

        type = (nodeRight instanceof Materialize || nodeLeft instanceof Materialize)
                ? JoinNodeType.NESTED_LOOP_MATERIALIZED : JoinNodeType.NESTED_LOOP;

        tupleRangeCalculator = new JoinTupleRangeCalculator(leftTable, rightTable,
                nodeLeft::getCosts, nodeRight::getCosts, type);
    }
}

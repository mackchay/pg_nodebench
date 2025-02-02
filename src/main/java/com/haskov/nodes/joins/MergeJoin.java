package com.haskov.nodes.joins;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.JoinCostCalculator;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeTreeData;
import com.haskov.tables.TableBuilder;
import com.haskov.types.JoinData;
import com.haskov.types.JoinNodeType;
import com.haskov.types.JoinType;
import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class MergeJoin implements Join {
    private Node nodeLeft;
    private Node nodeRight;

    private String leftTable;
    private String rightTable;
    private List<String> rightTableColumns;
    private List<String> leftTableColumns;

    private int leftConditionsCount;
    private int rightConditionsCount;
    private final JoinCostCalculator costCalculator = new JoinCostCalculator();


    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {

        Collections.shuffle(leftTableColumns);
        Collections.shuffle(rightTableColumns);
        List<String> selectColumns = qb.getSelectColumns();

        qb.join(new JoinData(
                        leftTable,
                        rightTable,
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

        if (rightCosts.getRight() > leftCosts.getRight()) {
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

        double totalCost = JoinCostCalculator.calculateMergeJoinCost(
                innerTable,
                outerTable,
                innerScanCost,
                outerScanCost,
                sel,
                sel,
                innerConditionsCount,
                outerConditionsCount
        );
        return new ImmutablePair<>(0.0, totalCost);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        Pair<Long, Long> leftTuplesRange = nodeLeft.getTuplesRange();
        Pair<Long, Long> rightTuplesRange = nodeRight.getTuplesRange();

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
                JoinNodeType.MERGE_JOIN
        );
        return new ImmutablePair<>(range.getLeft(), range.getRight());
    }

    @Override
    public List<String> getTables() {
        return List.of(leftTable, rightTable);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
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

        this.leftConditionsCount = nodeLeft.getConditions().getLeft() + nodeLeft.getConditions().getRight();
        this.rightConditionsCount = nodeRight.getConditions().getLeft() + nodeRight.getConditions().getRight();
    }
}

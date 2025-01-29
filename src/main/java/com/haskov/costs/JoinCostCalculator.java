package com.haskov.costs;

import com.haskov.types.JoinNodeType;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.haskov.bench.V2.sql;
import static com.haskov.costs.CostParameters.*;

public class JoinCostCalculator {
    private Map<JoinCacheData, Pair<Long, Long>> cache = new HashMap<>();

    //Nested Loop

    public static double calculateNestedLoopCost(String innerTableName, String outerTableName,
                                                 double innerTableScanCost, double outerTableScanCost,
                                                 double innerSel, double outerSel, double startScanCost,
                                                 int innerConditionCount, int outerConditionCount) {
        double innerNumTuples, outerNumTuples;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName);
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName);
        return getNestedLoopCost(innerNumTuples, outerNumTuples,
                innerTableScanCost, outerTableScanCost,
                innerSel, outerSel, startScanCost, innerConditionCount, outerConditionCount);
    }

    public static double getNestedLoopCost(double innerNumTuples, double outerNumTuples,
                                           double innerTableScanCost, double outerTableScanCost,
                                           double innerSel, double outerSel,
                                           double startScanCost,
                                           int innerConditionCount, int outerConditionCount) {
        double startUpCost, runCost;
        innerNumTuples = Math.max(Math.round(innerNumTuples * Math.pow(innerSel, innerConditionCount)), 1);
        outerNumTuples = Math.max(Math.round(outerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);
        startUpCost = 0;
        runCost = (cpuOperatorCost + cpuTupleCost) * innerNumTuples * outerNumTuples +
                innerTableScanCost * outerNumTuples + outerTableScanCost;
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    //Nested Loop Materialized

    public static double calculateMaterializeCost(double innerNumTuples, double innerTableScanCost) {
        double startUpCost, runCost;
        startUpCost = 0;
        runCost = 2 * cpuOperatorCost * innerNumTuples;
        return (double) Math.round(startUpCost * 100) / 100
                + innerTableScanCost
                + (double) Math.round(runCost * 100) / 100;
    }

    public static double calculateMaterializedNestedLoopCost(String innerTableName, String outerTableName,
                                                             double innerTableScanCost, double outerTableScanCost,
                                                             double innerSel, double outerSel,
                                                             double startScanCost,
                                                             int innerConditionCount, int outerConditionCount) {
        double innerNumTuples, outerNumTuples;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName);
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName);
        return getMaterializedNestedLoopCost(innerNumTuples, outerNumTuples,
                innerTableScanCost, outerTableScanCost,
                innerSel, outerSel,
                startScanCost, innerConditionCount, outerConditionCount);
    }

    public static double getMaterializedNestedLoopCost(double innerNumTuples, double outerNumTuples,
                                                       double innerTableScanCost, double outerTableScanCost,
                                                       double innerSel, double outerSel,
                                                       double startScanCost,
                                                       int innerConditionCount, int outerConditionCount) {
        double startUpCost, runCost, rescanCost;
        startUpCost = 0;
        innerNumTuples = Math.max(Math.round(innerNumTuples * Math.pow(innerSel, innerConditionCount)), 1);
        outerNumTuples = Math.max(Math.round(outerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);

        rescanCost = cpuOperatorCost * innerNumTuples;
        runCost = (cpuTupleCost + cpuOperatorCost) * innerNumTuples * outerNumTuples +
                rescanCost * (outerNumTuples - 1) + outerTableScanCost + calculateMaterializeCost(innerNumTuples,
                innerTableScanCost);
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    //TODO add index nested loop
    //Index Nested Loop

    public static double calculateIndexedNestedLoopJoinCost(String innerTableName, String outerTableName,
                                                        double innerTableIndexScanCost, double outerTableScanCost,
                                                        double sel) {
        double totalCost, outerNumTuples;
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * sel;
        totalCost = (cpuTupleCost + innerTableIndexScanCost) * outerNumTuples + outerTableScanCost;
        return (double) Math.round(totalCost * 100) / 100;
    }

    // Hash Join

    public static double calculateHashJoinCost(String innerTableName, String outerTableName,
                                               double innerTableScanCost, double outerTableScanCost,
                                               double innerSel, double outerSel, double startScanCost,
                                               int innerConditionCount, int outerConditionCount) {
        double innerNumTuples, outerNumTuples;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName);
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName);
        return getHashJoinCost(innerNumTuples, outerNumTuples, innerTableScanCost, outerTableScanCost,
                innerSel, outerSel,
                startScanCost, innerConditionCount, outerConditionCount);
    }

    public static double getHashJoinCost(double innerNumTuples, double outerNumTuples,
                                         double innerTableScanCost, double outerTableScanCost,
                                         double innerSel, double outerSel,
                                         double startScanCost,
                                         int innerConditionCount, int outerConditionCount) {
        double startUpCost, runCost,
                hashFunInnerCost, hashFunOuterCost, insertTupleCost,
                resultTupleCost, rescanCost;

        //Expected table with (0,1,..n) columns and same size of tables.
        innerNumTuples = Math.max(Math.round(innerNumTuples * Math.pow(innerSel, innerConditionCount)), 1);
        outerNumTuples = Math.max(Math.round(outerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);

        hashFunInnerCost = cpuOperatorCost * innerNumTuples;
        insertTupleCost = cpuTupleCost * innerNumTuples;
        startUpCost = innerTableScanCost + hashFunInnerCost + insertTupleCost + startScanCost;


        hashFunOuterCost = cpuOperatorCost * outerNumTuples;

        //TODO fix rescanCost
        double resultTuples = Math.max((long)(innerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);
        rescanCost = cpuOperatorCost * (innerNumTuples + outerNumTuples) * 0.150112;
        //rescanCost = 0;

        resultTupleCost = cpuTupleCost * resultTuples;
        runCost = outerTableScanCost + hashFunOuterCost + rescanCost +
                resultTupleCost;
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    // Merge Join

    public static double calculateMergeJoinCost(String innerTableName, String outerTableName,
                                            double innerTableScanCost, double outerTableScanCost,
                                                double innerSel, double outerSel,
                                            int innerConditionCount, int outerConditionCount) {
        double innerNumTuples, outerNumTuples;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName) * innerSel;
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * outerSel;
        return getMergeJoinCost(innerNumTuples, outerNumTuples, innerTableScanCost, outerTableScanCost,
                innerSel, outerSel,
                innerConditionCount, outerConditionCount);
    }

    public static double getMergeJoinCost(double innerNumTuples, double outerNumTuples,
                                          double innerTableScanCost, double outerTableScanCost,
                                          double innerSel, double outerSel,
                                          int innerConditionCount, int outerConditionCount) {
        double startUpCost, runCost, innerResSel = innerSel, outerResSel = outerSel;
        startUpCost = 0;
        if (innerConditionCount > 0) {
            innerResSel = Math.pow(innerSel, innerConditionCount);
        }
        if (outerConditionCount > 0) {
            outerResSel = Math.pow(outerSel, outerConditionCount);
        }
        double resultTuples = (innerNumTuples / innerSel) * innerResSel * outerResSel;
        runCost = innerTableScanCost + outerTableScanCost + cpuTupleCost *
                (innerNumTuples / innerSel) * innerResSel * outerResSel
                + cpuOperatorCost * (innerNumTuples + outerNumTuples);
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    // Min, max tuples functions

    public Pair<Long, Long> calculateTuplesRange(String innerTableName, String outerTableName,
                                     double innerTableScanCost, double outerTableScanCost,
                                     double startScanCost, int innerConditionCount,
                                     int outerConditionCount, JoinNodeType type) {
        JoinCacheData data = new JoinCacheData(
                type.toString(),
                innerTableScanCost,
                outerTableScanCost,
                startScanCost,
                innerConditionCount,
                outerConditionCount
        );
        if (cache.containsKey(data)) {
            return cache.get(data);
        }

        double innerNumTuples, outerNumTuples, sel;

        innerNumTuples = SQLUtils.getTableRowCount(innerTableName);
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName);

        List<Pair<JoinNodeType, Long>> rangeList = new ArrayList<>();

        rangeList.add(new ImmutablePair<>(JoinNodeType.NESTED_LOOP, 1L));

        for (int i = 2; i <= innerNumTuples; i++) {
            sel = (double) i / innerNumTuples;

            List<JoinNodeType> types = new ArrayList<>(List.of(
                    JoinNodeType.NESTED_LOOP,
                    JoinNodeType.NESTED_LOOP_MATERIALIZED,
                    JoinNodeType.HASH_JOIN,
                    JoinNodeType.MERGE_JOIN));

            boolean isIndexed = false;
            if (SQLUtils.hasIndexOnTable(innerTableName) && SQLUtils.hasIndexOnTable(outerTableName)) {
                isIndexed = true;
            }

            List<Double> costs = new ArrayList<>(List.of(
                    getNestedLoopCost(innerNumTuples, outerNumTuples,
                            innerTableScanCost, outerTableScanCost, sel, sel,
                            startScanCost, innerConditionCount, outerConditionCount),
                    getMaterializedNestedLoopCost(innerNumTuples, outerNumTuples,
                            innerTableScanCost, outerTableScanCost, sel, sel,
                            startScanCost, innerConditionCount, outerConditionCount),
                    getHashJoinCost(innerNumTuples, outerNumTuples,
                            innerTableScanCost, outerTableScanCost, sel, sel,
                            startScanCost, innerConditionCount, outerConditionCount),
                    getMergeJoinCost(innerNumTuples * sel, outerNumTuples * sel,
                            innerTableScanCost, outerTableScanCost, sel, sel, innerConditionCount, outerConditionCount)
                    )
            );

            if (!isIndexed || sel < 0.15) {
                costs.removeLast();
                types.remove(JoinNodeType.MERGE_JOIN);
            }
            if (type.equals(JoinNodeType.NESTED_LOOP) || type.equals(JoinNodeType.NESTED_LOOP_MATERIALIZED)) {
                costs.remove(2);
                types.remove(2);
                double newSel = innerNumTuples * Math.pow(sel, Math.max(innerConditionCount, outerConditionCount));
                if (innerNumTuples * Math.pow(sel, Math.max(innerConditionCount, outerConditionCount)) < 1.5) {
                    costs.remove(1);
                    types.remove(1);
                }
            }


            Map<JoinNodeType, Double> mapCosts = IntStream.range(0, types.size())
                    .boxed()
                    .collect(Collectors.toMap(types::get, costs::get));

            double minValue = Double.MAX_VALUE;
            JoinNodeType minKey = null;
            for (var entry : mapCosts.entrySet()) {
                if (entry.getValue() < minValue) {
                    minValue = entry.getValue();
                    minKey = entry.getKey();
                }
            }
            rangeList.add(new ImmutablePair<>(minKey, (long)(i)));
        }
        List<Long> costList = rangeList.stream().filter(pair -> pair.getKey().equals(type))
                .map(Pair::getValue).toList();

        Pair<Long, Long> range = new ImmutablePair<>(
                (long)(costList.getFirst()),
                (long) (costList.getLast())
        );
        cache.put(data, range);
        return range;
    }

}

package com.haskov.costs;

import com.haskov.types.JoinNodeType;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.haskov.costs.CostParameters.*;

public class JoinCostCalculator {
    private Map<JoinCacheData, Pair<Long, Long>> cache = new HashMap<>();

    //Nested Loop

    public static double calculateNestedLoopCost(String innerTableName, String outerTableName,
                                                 double innerTableScanCost, double outerTableScanCost,
                                                 double innerSel, double outerSel) {
        double innerNumTuples, outerNumTuples;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName) * innerSel;
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * outerSel;
        return getNestedLoopCost(innerNumTuples, outerNumTuples, innerTableScanCost, outerTableScanCost);
    }

    public static double getNestedLoopCost(double innerNumTuples, double outerNumTuples,
                                           double innerTableScanCost, double outerTableScanCost) {
        double startUpCost, runCost;
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
                                                             double innerSel, double outerSel) {
        double innerNumTuples, outerNumTuples;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName) * innerSel;
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * outerSel;
        return getMaterializedNestedLoopCost(innerNumTuples, outerNumTuples, innerTableScanCost, outerTableScanCost);
    }

    public static double getMaterializedNestedLoopCost(double innerNumTuples, double outerNumTuples,
                                                       double innerTableScanCost, double outerTableScanCost) {
        double startUpCost, runCost, rescanCost;
        startUpCost = 0;
        rescanCost = cpuOperatorCost * innerNumTuples;
        runCost = (cpuOperatorCost + cpuTupleCost) * innerNumTuples * outerNumTuples +
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
                resultTupleCost = 0, rescanCost,
                innerResSel = innerSel, outerResSel = outerSel;

        //Expected table with (0,1,..n) columns and same size of tables.
        if (innerConditionCount > 0) {
            innerResSel = Math.pow(innerSel, innerConditionCount);
        }
        if (outerConditionCount > 0) {
            outerResSel = Math.pow(outerSel, outerConditionCount);
        }

        hashFunInnerCost = cpuOperatorCost * innerNumTuples;
        insertTupleCost = cpuTupleCost * innerNumTuples * innerResSel;
        startUpCost = innerTableScanCost + hashFunInnerCost + insertTupleCost + startScanCost;

        outerNumTuples = outerNumTuples * outerResSel;
        hashFunOuterCost = cpuOperatorCost * outerNumTuples;

        //TODO fix rescanCost
        double resultTuples = innerNumTuples * innerResSel * outerResSel;
        rescanCost = cpuTupleCost * resultTuples * 0.1;
        //rescanCost = 0;

        resultTupleCost += cpuTupleCost * resultTuples;
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

    public Pair<Long, Long> calculateHashJoinTuplesRange(String innerTableName, String outerTableName,
                                                                      double innerTableScanCost, double outerTableScanCost,
                                                                      double startScanCost, int innerConditionCount,
                                                                int outerConditionCount) {
        JoinCacheData data = new JoinCacheData(
                "HashJoin",
                innerTableScanCost,
                outerTableScanCost,
                startScanCost,
                innerConditionCount,
                outerConditionCount
        );
        if (cache.containsKey(data)) {
            return cache.get(data);
        }

        Long numTuples = SQLUtils.getTableRowCount(innerTableName);
        double sel = (double) 1 / numTuples;
        double minNestedLoopCost = Math.min(
                calculateNestedLoopCost(innerTableName, outerTableName, innerTableScanCost,
                        outerTableScanCost, sel, sel),
                calculateMaterializedNestedLoopCost(innerTableName, outerTableName,
                        innerTableScanCost, outerTableScanCost, sel, sel)
        );

        while (calculateHashJoinCost(innerTableName, outerTableName, innerTableScanCost, outerTableScanCost,
                sel, sel, startScanCost, innerConditionCount, outerConditionCount)
                > minNestedLoopCost) {
            sel *= 1.1;
            minNestedLoopCost = Math.min(
                    calculateNestedLoopCost(innerTableName, outerTableName, innerTableScanCost,
                            outerTableScanCost, sel, sel),
                    calculateMaterializedNestedLoopCost(innerTableName, outerTableName,
                            innerTableScanCost, outerTableScanCost, sel, sel)
            );
        }

        Pair<Long, Long> range = new ImmutablePair<>((long)((sel + 0.05) * numTuples), numTuples);
        cache.put(data, range);
        return range;
    }

    public Pair<Long, Long> calculateNestedLoopTuplesRange(String innerTableName, String outerTableName,
                                                                  double innerTableScanCost, double outerTableScanCost,
                                                                  double startScanCost, int innerConditionCount,
                                                                  int outerConditionCount) {
        JoinCacheData data = new JoinCacheData(
                "NestedLoop",
                innerTableScanCost,
                outerTableScanCost,
                startScanCost,
                innerConditionCount,
                outerConditionCount
        );
        if (cache.containsKey(data)) {
            return cache.get(data);
        }

        Long numTuples = SQLUtils.getTableRowCount(innerTableName);
        double sel = 1;
        double nestedLoopCost = calculateNestedLoopCost(innerTableName, outerTableName, innerTableScanCost,
                outerTableScanCost, sel, sel);
        while (calculateHashJoinCost(innerTableName, outerTableName, innerTableScanCost, outerTableScanCost,
                sel, sel, startScanCost, innerConditionCount, outerConditionCount)
                < calculateNestedLoopCost(innerTableName, outerTableName, innerTableScanCost,
                outerTableScanCost, sel, sel)
        ) {
            sel *= 0.9;
        }

        double materializedNestedLoopCost = calculateMaterializedNestedLoopCost(innerTableName, outerTableName,
                innerTableScanCost, outerTableScanCost, sel, sel);
        while (calculateMaterializedNestedLoopCost(innerTableName, outerTableName,
                innerTableScanCost, outerTableScanCost, sel, sel)
                < calculateNestedLoopCost(innerTableName, outerTableName, innerTableScanCost,
                outerTableScanCost, sel, sel)) {
            sel *= 0.9;
        }

        Pair<Long, Long> range = new ImmutablePair<>(1L, (long)(sel * numTuples));
        cache.put(data, range);
        return range;
    }

    public Pair<Long, Long> calculateMaterializedNestedLoopTuplesRange(String innerTableName, String outerTableName,
                                                                  double innerTableScanCost, double outerTableScanCost,
                                                                  double startScanCost, int innerConditionCount,
                                                                  int outerConditionCount) {
        JoinCacheData data = new JoinCacheData(
                "MaterializedNestedLoop",
                innerTableScanCost,
                outerTableScanCost,
                startScanCost,
                innerConditionCount,
                outerConditionCount
        );

        if (cache.containsKey(data)) {
            return cache.get(data);
        }

        Long numTuples = SQLUtils.getTableRowCount(innerTableName);
        double minSel = (double) 1 / numTuples;
        double maxSel = 1;

        while (calculateHashJoinCost(innerTableName, outerTableName, innerTableScanCost, outerTableScanCost,
                maxSel, maxSel, startScanCost, innerConditionCount, outerConditionCount)
                < calculateMaterializedNestedLoopCost(innerTableName, outerTableName, innerTableScanCost,
                outerTableScanCost, maxSel, maxSel)
        ) {
            maxSel *= 0.98;
        }

        while (calculateMaterializedNestedLoopCost(innerTableName, outerTableName,
                innerTableScanCost, outerTableScanCost, minSel, minSel)
                > calculateNestedLoopCost(innerTableName, outerTableName, innerTableScanCost,
                outerTableScanCost, minSel, minSel)) {
            minSel *= 1.02;
        }


        Pair<Long, Long> range = new ImmutablePair<>((long)(minSel * numTuples), (long)((maxSel) * numTuples));
        cache.put(data, range);
        return range;
    }

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

        List<JoinNodeType> types = new ArrayList<>(List.of(
                JoinNodeType.NESTED_LOOP,
                JoinNodeType.NESTED_LOOP_MATERIALIZED,
                JoinNodeType.HASH_JOIN,
                JoinNodeType.MERGE_JOIN));

        boolean isIndexed = false;
        if (SQLUtils.hasIndexOnTable(innerTableName) && SQLUtils.hasIndexOnTable(outerTableName)) {
            isIndexed = true;
        } else {
            types.remove(JoinNodeType.MERGE_JOIN);
        }

        for (int i = 1; i <= innerNumTuples; i++) {
            sel = (double) i / innerNumTuples;
            List<Double> costs = new ArrayList<>(List.of(
                    getNestedLoopCost(innerNumTuples * sel, outerNumTuples * sel,
                            innerTableScanCost, outerTableScanCost),
                    getMaterializedNestedLoopCost(innerNumTuples * sel, outerNumTuples * sel,
                            innerTableScanCost, outerTableScanCost),
                    getHashJoinCost(innerNumTuples, outerNumTuples,
                            innerTableScanCost, outerTableScanCost, sel, sel,
                            startScanCost, innerConditionCount, outerConditionCount),
                    getMergeJoinCost(innerNumTuples * sel, outerNumTuples * sel,
                            innerTableScanCost, outerTableScanCost, sel, sel, innerConditionCount, outerConditionCount)
                    )
            );

            if (!isIndexed) {
                costs.removeLast();
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
            rangeList.add(new ImmutablePair<>(minKey, (long)(innerNumTuples * sel)));
        }
        List<Long> costList = rangeList.stream().filter(pair -> pair.getKey().equals(type))
                .map(Pair::getValue).toList();

        Pair<Long, Long> range = new ImmutablePair<>(costList.getFirst(), costList.getLast());
        cache.put(data, range);
        return range;
    }

}

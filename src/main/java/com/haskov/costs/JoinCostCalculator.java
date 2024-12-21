package com.haskov.costs;

import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;

import static com.haskov.costs.CostParameters.*;

public class JoinCostCalculator {
    private static Map<JoinCacheData, Pair<Long, Long>> cache = new HashMap<>();

    public static double calculateNestedLoopCost(String innerTableName, String outerTableName,
                                                 double innerTableScanCost, double outerTableScanCost,
                                                 double innerSel, double outerSel) {
        double startUpCost, runCost, innerNumTuples, outerNumTuples;
        startUpCost = 0;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName) * innerSel;
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * outerSel;
        runCost = (cpuOperatorCost + cpuTupleCost) * innerNumTuples * outerNumTuples +
                innerTableScanCost * outerNumTuples + outerTableScanCost;
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    public static double calculateMaterializeCost(double innerNumTuples, double innerTableScanCost,
                                                  double sel) {
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
        double startUpCost, runCost, innerNumTuples, outerNumTuples, rescanCost;
        startUpCost = 0;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName) * innerSel;
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * outerSel;
        rescanCost = cpuOperatorCost * innerNumTuples;
        runCost = (cpuOperatorCost + cpuTupleCost) * innerNumTuples * outerNumTuples +
                rescanCost * (outerNumTuples - 1) + outerTableScanCost + calculateMaterializeCost(innerNumTuples,
                innerTableScanCost, innerSel);
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    public static double calculateIndexedNestedLoopJoinCost(String innerTableName, String outerTableName,
                                                        double innerTableIndexScanCost, double outerTableScanCost,
                                                        double sel) {
        double totalCost, outerNumTuples;
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * sel;
        totalCost = (cpuTupleCost + innerTableIndexScanCost) * outerNumTuples + outerTableScanCost;
        return (double) Math.round(totalCost * 100) / 100;
    }

    public static double calculateHashJoinCost(String innerTableName, String outerTableName,
                                               double innerTableScanCost, double outerTableScanCost,
                                               double innerSel, double outerSel, double startScanCost,
                                               int innerConditionCount, int outerConditionCount) {
        double startUpCost, runCost, innerNumTuples, outerNumTuples,
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

        innerNumTuples = SQLUtils.getTableRowCount(innerTableName);
        hashFunInnerCost = cpuOperatorCost * innerNumTuples;
        insertTupleCost = cpuTupleCost * innerNumTuples;
        startUpCost = innerTableScanCost + hashFunInnerCost + insertTupleCost + startScanCost;

        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * outerResSel;
        hashFunOuterCost = cpuOperatorCost * outerNumTuples;

        //TODO fix rescanCost
        rescanCost = cpuOperatorCost * outerNumTuples * (0.5 / (innerNumTuples)) * innerNumTuples;
        //rescanCost = 0;

        resultTupleCost += cpuTupleCost * innerResSel * outerResSel * Math.min(innerNumTuples, outerNumTuples);
        runCost = outerTableScanCost + hashFunOuterCost + rescanCost +
                resultTupleCost;
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    // Min, max tuples functions

    public static Pair<Long, Long> calculateHashJoinTuplesRange(String innerTableName, String outerTableName,
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

    public static Pair<Long, Long> calculateNestedLoopTuplesRange(String innerTableName, String outerTableName,
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

    public static Pair<Long, Long> calculateMaterializedNestedLoopTuplesRange(String innerTableName, String outerTableName,
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
}

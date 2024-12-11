package com.haskov.costs;

import com.haskov.utils.SQLUtils;

import static com.haskov.costs.CostParameters.*;

public class JoinCostCalculator {

    public static double calculateNestedLoopCost(String innerTableName, String outerTableName,
                                                 double innerTableScanCost, double outerTableScanCost,
                                                 double sel) {
        double startUpCost, runCost, innerNumTuples, outerNumTuples;
        startUpCost = 0;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName) * sel;
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * sel;
        runCost = (cpuOperatorCost + cpuTupleCost) * innerNumTuples * outerNumTuples +
                innerTableScanCost * outerNumTuples + outerTableScanCost;
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    public static double calculateMaterializeCost(String innerTableName, double innerTableScanCost,
                                                  double sel) {
        double startUpCost, runCost, innerNumTuples;
        startUpCost = 0;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName) * sel;
        runCost = 2 * cpuOperatorCost * innerNumTuples;
        return (double) Math.round(startUpCost * 100) / 100
                + innerTableScanCost
                + (double) Math.round(runCost * 100) / 100;
    }

    public static double calculateMaterializedNestedLoopCost(String innerTableName, String outerTableName,
                                                             double innerTableScanCost, double outerTableScanCost,
                                                             double sel) {
        double startUpCost, runCost, innerNumTuples, outerNumTuples, rescanCost;
        startUpCost = 0;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName) * sel;
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * sel;
        rescanCost = cpuOperatorCost * innerNumTuples;
        runCost = (cpuOperatorCost + cpuTupleCost) * innerNumTuples * outerNumTuples +
                rescanCost * (outerNumTuples - 1) + outerTableScanCost + calculateMaterializeCost(innerTableName,
                innerTableScanCost, sel);
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
                                               double sel, double startScanCost, int conditionCount) {
        double startUpCost, runCost, innerNumTuples, outerNumTuples,
                hashFunInnerCost, hashFunOuterCost, insertTupleCost,
                resultTupleCost, rescanCost;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName) * sel;
        hashFunInnerCost = cpuOperatorCost * innerNumTuples;
        insertTupleCost = cpuTupleCost * innerNumTuples;
        startUpCost = innerTableScanCost + hashFunInnerCost + insertTupleCost + startScanCost;

        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * sel;
        hashFunOuterCost = cpuOperatorCost * outerNumTuples;

        //TODO fix rescanCost
        //rescanCost = cpuOperatorCost * innerNumTuples * outerNumTuples;
        rescanCost = 0;

        //Expected table with (0,1,..n) columns and same size of tables.
        resultTupleCost = cpuTupleCost * Math.pow(sel, conditionCount) * innerNumTuples;

        runCost = outerTableScanCost + hashFunOuterCost + rescanCost +
                resultTupleCost;
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }
}

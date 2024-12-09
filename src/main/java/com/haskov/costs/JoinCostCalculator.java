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
        return startUpCost + runCost;
    }

    public static double calculateMaterializeCost(String innerTableName, double innerTableScanCost,
                                                  double sel) {
        double startUpCost, runCost, innerNumTuples;
        startUpCost = 0;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName) * sel;
        runCost = 2 * cpuOperatorCost * innerNumTuples;
        return startUpCost + runCost + innerTableScanCost;
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
        return startUpCost + runCost;
    }

    public static double calculateIndexedNestedLoopJoin(String innerTableName, String outerTableName,
                                                        double innerTableIndexScanCost, double outerTableScanCost,
                                                        double sel) {
        double totalCost, outerNumTuples;
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName) * sel;
        totalCost = (cpuTupleCost + innerTableIndexScanCost) * outerNumTuples + outerTableScanCost;
        return totalCost;
    }

}

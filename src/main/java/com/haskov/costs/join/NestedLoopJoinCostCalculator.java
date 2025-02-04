package com.haskov.costs.join;

import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.Pair;

import static com.haskov.costs.CostParameters.cpuOperatorCost;
import static com.haskov.costs.CostParameters.cpuTupleCost;
import static com.haskov.utils.SQLUtils.getTableRowCount;

public class NestedLoopJoinCostCalculator {
    private final long innerNumTuples;
    private final long outerNumTuples;


    /**
     * @param innerTable     Название внутренней таблицы.
     * @param outerTable     Название внешней таблицы.
     */
    public NestedLoopJoinCostCalculator(String innerTable, String outerTable) {
        innerNumTuples = getTableRowCount(innerTable);
        outerNumTuples = getTableRowCount(outerTable);
    }

    /**
     * @param innerNumTuples     Количество строк во внутренней таблице.
     * @param outerNumTuples     Количество строк во внешней таблице.
     */
    NestedLoopJoinCostCalculator(long innerNumTuples, long outerNumTuples) {
        this.innerNumTuples = innerNumTuples;
        this.outerNumTuples = outerNumTuples;
    }

    /**
     * Алгоритм вычисления стоимости вложенного цикла (Nested Loop Join).
     *
     * @param innerTableScanCost Стоимость сканирования внутренней таблицы.
     * @param outerTableScanCost Стоимость сканирования внешней таблицы.
     * @param innerSel           Селективность внутренних условий.
     * @param outerSel           Селективность внешних условий.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @return Общая стоимость выполнения соединения.
     */
    public double calculateCost(double innerTableScanCost, double outerTableScanCost,
                                           double innerSel, double outerSel,
                                           int innerConditionCount, int outerConditionCount) {
        double startUpCost, runCost;
        long innerNumTuplesSel, outerNumTuplesSel;
        innerNumTuplesSel = Math.max(Math.round(innerNumTuples * Math.pow(innerSel, innerConditionCount)), 1);
        outerNumTuplesSel = Math.max(Math.round(outerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);
        startUpCost = 0;
        runCost = (cpuOperatorCost + cpuTupleCost) * innerNumTuplesSel * outerNumTuplesSel +
                innerTableScanCost * outerNumTuplesSel + outerTableScanCost;
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    /**
     * Вычисляет стоимость узла Materialize (Materialize).
     *
     * @param innerNumTuples      Количество строк во внутренней таблице.
     * @param innerTableScanCost  Стоимость сканирования внутренней таблицы.
     * @return Общая стоимость узла.
     */
    public static double calculateMaterializeCost(double innerNumTuples, double innerTableScanCost) {
        double startUpCost, runCost;
        startUpCost = 0;
        runCost = 2 * cpuOperatorCost * innerNumTuples;
        return (double) Math.round(startUpCost * 100) / 100
                + innerTableScanCost
                + (double) Math.round(runCost * 100) / 100;
    }

    /**
     * Алгоритм вычисления стоимости вложенного цикла с узлом Materialize (Nested Loop Join).
     *
     * @param innerTableScanCost Стоимость сканирования внутренней таблицы.
     * @param outerTableScanCost Стоимость сканирования внешней таблицы.
     * @param innerSel           Селективность внутренних условий.
     * @param outerSel           Селективность внешних условий.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @return Общая стоимость выполнения соединения.
     */
    public double calculateMaterializedCost(double innerTableScanCost, double outerTableScanCost,
                                                       double innerSel, double outerSel,
                                                       int innerConditionCount, int outerConditionCount) {
        double startUpCost, runCost, rescanCost;
        long innerNumTuplesSel, outerNumTuplesSel;
        startUpCost = 0;
        innerNumTuplesSel = Math.max(Math.round(innerNumTuples * Math.pow(innerSel, innerConditionCount)), 1);
        outerNumTuplesSel = Math.max(Math.round(outerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);

        rescanCost = cpuOperatorCost * innerNumTuplesSel;
        runCost = (cpuTupleCost + cpuOperatorCost) * innerNumTuplesSel * outerNumTuplesSel +
                rescanCost * (outerNumTuplesSel - 1) + outerTableScanCost + calculateMaterializeCost(innerNumTuplesSel,
                innerTableScanCost);
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }
}

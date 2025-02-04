package com.haskov.costs.join;

import com.haskov.utils.SQLUtils;

import static com.haskov.costs.CostParameters.cpuOperatorCost;
import static com.haskov.costs.CostParameters.cpuTupleCost;
import static com.haskov.utils.SQLUtils.getTableRowCount;

public class HashJoinCostCalculator {
    private final long innerNumTuples;
    private final long outerNumTuples;


    /**
     * @param innerTable     Название внутренней таблицы.
     * @param outerTable     Название внешней таблицы.
     */
    public HashJoinCostCalculator(String innerTable, String outerTable) {
        innerNumTuples = getTableRowCount(innerTable);
        outerNumTuples = getTableRowCount(outerTable);
    }

    /**
     * @param innerNumTuples     Количество строк во внутренней таблице.
     * @param outerNumTuples     Количество строк во внешней таблице.
     */
    HashJoinCostCalculator(long innerNumTuples, long outerNumTuples) {
        this.innerNumTuples = innerNumTuples;
        this.outerNumTuples = outerNumTuples;
    }

    /**
     * Алгоритм вычисления стоимости соединения хешированием (Hash Join).
     *
     * @param innerTableScanCost Стоимость сканирования внутренней таблицы.
     * @param outerTableScanCost Стоимость сканирования внешней таблицы.
     * @param innerSel           Селективность внутренних условий.
     * @param outerSel           Селективность внешних условий.
     * @param startScanCost      Начальная стоимость сканирования.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @return Общая стоимость выполнения соединения.
     */
    public double calculateCost(double innerTableScanCost, double outerTableScanCost,
                                         double innerSel, double outerSel,
                                         int innerConditionCount, int outerConditionCount, double startScanCost) {
        double startUpCost, runCost,
                hashFunInnerCost, hashFunOuterCost, insertTupleCost,
                resultTupleCost, rescanCost;
        long innerNumTuplesSel, outerNumTuplesSel;

        innerNumTuplesSel = Math.max(Math.round(innerNumTuples * Math.pow(innerSel, innerConditionCount)), 1);
        outerNumTuplesSel = Math.max(Math.round(outerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);

        hashFunInnerCost = cpuOperatorCost * innerNumTuplesSel;
        insertTupleCost = cpuTupleCost * innerNumTuplesSel;
        startUpCost = innerTableScanCost + hashFunInnerCost + insertTupleCost + startScanCost;


        hashFunOuterCost = cpuOperatorCost * outerNumTuplesSel;

        //TODO fix rescanCost
        double resultTuples = Math.max((long)(innerNumTuplesSel * Math.pow(outerSel, outerConditionCount)), 1);
        rescanCost = cpuOperatorCost * (innerNumTuplesSel + outerNumTuplesSel) * 0.150112;
        //rescanCost = 0;

        resultTupleCost = cpuTupleCost * resultTuples;
        runCost = outerTableScanCost + hashFunOuterCost + rescanCost +
                resultTupleCost;
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

}

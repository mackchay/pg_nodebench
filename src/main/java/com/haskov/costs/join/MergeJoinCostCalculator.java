package com.haskov.costs.join;

import static com.haskov.costs.CostParameters.cpuOperatorCost;
import static com.haskov.costs.CostParameters.cpuTupleCost;
import static com.haskov.utils.SQLUtils.getTableRowCount;

public class MergeJoinCostCalculator {
    private final long innerNumTuples;
    private final long outerNumTuples;

    /**
     * @param innerTable     Название внутренней таблицы.
     * @param outerTable     Название внешней таблицы.
     */
    public MergeJoinCostCalculator(String innerTable, String outerTable) {
        innerNumTuples = getTableRowCount(innerTable);
        outerNumTuples = getTableRowCount(outerTable);
    }

    /**
     * @param innerNumTuples     Количество строк во внутренней таблице.
     * @param outerNumTuples     Количество строк во внешней таблице.
     */
    MergeJoinCostCalculator(long innerNumTuples, long outerNumTuples) {
        this.innerNumTuples = innerNumTuples;
        this.outerNumTuples = outerNumTuples;
    }


    /**
     * Алгоритм вычисления стоимости соединения слиянием (Merge Join).
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
        startUpCost = 0;
        innerNumTuplesSel = Math.max(Math.round(innerNumTuples * Math.pow(innerSel, innerConditionCount)), 1);
        outerNumTuplesSel = Math.max(Math.round(outerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);

        double resultTuples = (innerNumTuplesSel / innerSel) * innerSel * outerSel;
        runCost = innerTableScanCost + outerTableScanCost + cpuTupleCost *
                (innerNumTuplesSel / innerSel) * innerSel * outerSel
                + cpuOperatorCost * (innerNumTuplesSel + outerNumTuplesSel);
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }
}

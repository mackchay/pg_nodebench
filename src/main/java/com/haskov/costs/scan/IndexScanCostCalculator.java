package com.haskov.costs.scan;

import org.apache.commons.lang3.tuple.Pair;

import static com.haskov.costs.CostParameters.*;
import static com.haskov.utils.SQLUtils.*;

public class IndexScanCostCalculator {
    private final long numPages;
    private final long numTuples;
    private final long numIndexPages;
    private final long numIndexTuples;
    private final long heightBTree;
    private final double correlation;

    public IndexScanCostCalculator(String tableName, String indexedColumn) {
        Pair<Long, Long> result = getTablePagesAndRowsCount(tableName);
        numPages = result.getLeft();
        numTuples = result.getRight();

        Pair<Long, Long> indexResult = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numIndexPages = indexResult.getLeft();
        numIndexTuples = indexResult.getRight();

        heightBTree = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));
        correlation = getCorrelation(tableName, indexedColumn);
    }

    IndexScanCostCalculator(long numPages, long numTuples,
                            long numIndexPages, long numIndexTuples,
                            long heightBTree, double correlation) {
        this.numPages = numPages;
        this.numTuples = numTuples;
        this.numIndexPages = numIndexPages;
        this.numIndexTuples = numIndexTuples;
        this.heightBTree = heightBTree;
        this.correlation = correlation;
    }

    public double calculateCost(int indexConditionsCount, int conditionsCount, double sel) {
        double startup, runCost, idxCpuCost, tableCpuCost, indexIOCost, tableIOCost,
                maxIOCost, minIOCost, idxCostPerTuple;

        startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (heightBTree + 1) * 50) * cpuOperatorCost;

        idxCostPerTuple = cpuIndexTupleCost + qualOpCost * indexConditionsCount;

        idxCpuCost = sel * numIndexTuples * (idxCostPerTuple);
        tableCpuCost = sel * numTuples * (cpuTupleCost + conditionsCount * cpuOperatorCost);
        indexIOCost = Math.ceil(sel * numIndexPages) * randomPageCost;

        maxIOCost = numPages  * randomPageCost;
        minIOCost = randomPageCost + (Math.ceil(sel * numPages) - 1) * seqPageCost;

        tableIOCost = maxIOCost +
                Math.pow(correlation, 2) * (minIOCost - maxIOCost);
        runCost = idxCpuCost + indexIOCost + tableCpuCost + tableIOCost;

        return (double) Math.round(startup * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }


    public double calculateStartUpCost() {
        return Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (heightBTree + 1) * 50) * cpuOperatorCost;
    }
}

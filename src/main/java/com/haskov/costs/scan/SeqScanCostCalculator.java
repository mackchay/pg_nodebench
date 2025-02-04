package com.haskov.costs.scan;

import org.apache.commons.lang3.tuple.Pair;

import static com.haskov.costs.CostParameters.*;
import static com.haskov.utils.SQLUtils.getTablePagesAndRowsCount;

public class SeqScanCostCalculator {
    private final long numPages;
    private final long numTuples;

    public SeqScanCostCalculator(String tableName) {
        Pair<Long, Long> result = getTablePagesAndRowsCount(tableName);
        numPages = result.getLeft();
        numTuples = result.getRight();
    }

    SeqScanCostCalculator(long numPages, long numTuples) {
        this.numPages = numPages;
        this.numTuples = numTuples;
    }

    public double calculateCost(int conditionsCount) {
        return (seqPageCost * numPages) + (cpuTupleCost * numTuples) + (cpuOperatorCost * conditionsCount * numTuples);
    }
}

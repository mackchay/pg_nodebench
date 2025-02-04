package com.haskov.costs.scan;

import org.apache.commons.lang3.tuple.Pair;

import static com.haskov.costs.CostParameters.*;
import static com.haskov.utils.SQLUtils.*;

public class BitmapScanCostCalculator {
    private final long numPages;
    private final long numTuples;
    private final long numIndexPages;
    private final long numIndexTuples;
    private final long heightBTree;

    public BitmapScanCostCalculator(String tableName, String indexedColumn) {
        Pair<Long, Long> result = getTablePagesAndRowsCount(tableName);
        numPages = result.getLeft();
        numTuples = result.getRight();

        Pair<Long, Long> indexResult = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numIndexPages = indexResult.getLeft();
        numIndexTuples = indexResult.getRight();

        heightBTree = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));
    }

    BitmapScanCostCalculator(long numPages, long numTuples, long numIndexPages, long numIndexTuples,
                             long heightBTree) {
        this.numPages = numPages;
        this.numTuples = numTuples;
        this.numIndexPages = numIndexPages;
        this.numIndexTuples = numIndexTuples;
        this.heightBTree = heightBTree;
    }

    public double calculateIndexCost(int indexConditionsCount, double sel) {
        double startup, runCost, idxCpuCost, indexIOCost, idxCostPerTuple;

        startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (heightBTree + 1) * 50) * cpuOperatorCost;
        //startup = 0;

        idxCostPerTuple = cpuIndexTupleCost + qualOpCost * indexConditionsCount;

        idxCpuCost = sel * numIndexTuples * (idxCostPerTuple);
        indexIOCost = Math.ceil(sel * numIndexPages) * randomPageCost;
        runCost = idxCpuCost + indexIOCost;
        return (double) Math.round(startup * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    public double calculateCost(int indexConditionsCount, int conditionsCount, double sel) {

        double idxCost = calculateIndexCost(indexConditionsCount, sel);

        double formula = 2 * numPages * numTuples * sel / (2 * numPages + numTuples * sel);
        double pagesFetched = Math.min(numPages, formula);
        double costPerPage = randomPageCost - (randomPageCost - seqPageCost) * Math.sqrt(pagesFetched / numPages);
        if (pagesFetched == 1) {
            costPerPage = randomPageCost;
        }
        double startUpCost = idxCost + sel * 0.1 * cpuOperatorCost
                * numIndexTuples * sel;
        double runCost = costPerPage * pagesFetched + cpuTupleCost * numTuples * sel
                + cpuOperatorCost * (conditionsCount + indexConditionsCount) * numTuples * sel;
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    public double calculateStartUpCost() {
        return Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (heightBTree + 1) * 50) * cpuOperatorCost;
    }
}

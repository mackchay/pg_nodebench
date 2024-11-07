package com.haskov.costs;

import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import static com.haskov.costs.CostParameters.*;
import static com.haskov.utils.SQLUtils.*;

public class ScanCostCalculator {

    public static double calculateSeqScanCost(String tableName, int conditionCount) {

        Pair<Long, Long> result = SQLUtils.getTablePagesAndRowsCount(tableName);
        double numPages = result.getLeft();
        double numTuples = result.getRight();
        return (seqPageCost * numPages) + (cpuTupleCost * numTuples) + (cpuOperatorCost * conditionCount * numTuples);
    }

    public static double calculateIndexScanCost(String tableName, String indexedColumn,
                                                int indexConditionsCount, int conditionsCount) {
        double idxCost, tblCost, numPages, numTuples;

        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numPages = resultIndexTable.getLeft();
        numTuples = resultIndexTable.getRight();
        idxCost = (randomPageCost * numPages) + (cpuIndexTupleCost * numTuples) +
                (cpuOperatorCost * indexConditionsCount * numTuples);

        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);
        numPages = resultTable.getLeft();
        numTuples = resultTable.getRight();
        if (getCorrelation(tableName, indexedColumn) > 0.5) {
            tblCost = (seqPageCost * numPages) + (cpuTupleCost * numTuples) *
                    (cpuOperatorCost * conditionsCount * numTuples);
        } else {
            tblCost = (randomPageCost * numTuples) + (cpuTupleCost * numTuples) *
                    (cpuOperatorCost * conditionsCount * numTuples);
        }

        return idxCost + tblCost;
    }

    public static double calculateIndexOnlyScanCost(String tableName, String indexedColumn,
                                                    int indexConditionsCount, int conditionsCount) {
        int visiblePages = getVisiblePages(tableName);
        double idxCost, tblCost, numPages, numTuples;

        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numPages = resultIndexTable.getLeft();
        numTuples = resultIndexTable.getRight();
        idxCost = (randomPageCost * numPages) + (cpuIndexTupleCost * numTuples) +
                    (cpuOperatorCost * indexConditionsCount * numTuples);


        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);
        numPages = resultTable.getLeft();
        numTuples = resultTable.getRight();
        double fracVisiblePages = (double) visiblePages / numPages;
        if (getCorrelation(tableName, indexedColumn) > 0.5) {
            tblCost = (1 - fracVisiblePages) * (seqPageCost * numPages) + (cpuTupleCost * numTuples)
            + (cpuOperatorCost * conditionsCount * numTuples);
        } else {
            tblCost = (1 - fracVisiblePages) * (randomPageCost * numTuples) + (cpuTupleCost * numTuples)
            + (cpuOperatorCost * conditionsCount * numTuples);
        }

        return idxCost + tblCost;
    }

    public static double calculateBitmapIndexScanCost(String tableName, String indexedColumn,
                                                      int indexConditionsCount) {
        double idxCost, numPages, numTuples;

        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numPages = resultIndexTable.getLeft();
        numTuples = resultIndexTable.getRight();
        idxCost = (randomPageCost * numPages) + (cpuIndexTupleCost * numTuples) +
                (cpuOperatorCost * indexConditionsCount * numTuples);

        return idxCost;
    }

    public static double calculateBitmapHeapAndIndexScanCost(String tableName, String indexedColumn,
                                                             int indexConditionsCount, int conditionsCount,
                                                             double sel) {
        double idxCost = calculateBitmapIndexScanCost(tableName, indexedColumn, indexConditionsCount) * sel;
        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);

        double numPages = resultTable.getLeft();
        double numTuples = resultTable.getRight() * sel;
        double indexNumTuples = getTableRowCount(getIndexOnColumn(tableName, indexedColumn)) * sel;
        double formula = 2 * numPages * numTuples / (2 * numPages + numTuples);
        double pagesFetched = Math.min(numPages, formula);
        double costPerPage = randomPageCost - (randomPageCost - seqPageCost) * Math.sqrt(pagesFetched / numPages);
        double startUpCost = idxCost + sel * 0.1 * cpuOperatorCost
                * indexNumTuples;
        double runCost = costPerPage * pagesFetched + cpuTupleCost * numTuples
                + cpuOperatorCost * (conditionsCount + indexConditionsCount) * numTuples;
        return startUpCost + runCost;
    }

    // Calculate max tuples

    public static Long calculateIndexOnlyScanMaxTuples(String tableName, String columnName,
                                                       int indexConditionsCount, int conditionsCount) {
        double seqScanCost = calculateSeqScanCost(tableName, conditionsCount + indexConditionsCount);
        double indexOnlyScanCost = calculateIndexOnlyScanCost(tableName, columnName, indexConditionsCount, conditionsCount);
        return (Long) (long) ((seqScanCost / (indexOnlyScanCost + 100)) * getTableRowCount(tableName));
    }

    public static Long calculateIndexScanMaxTuples(String tableName, String columnName,
                                                   int conditionsCount, int indexConditionsCount) {
        double seqScanCost = calculateSeqScanCost(tableName, conditionsCount + indexConditionsCount);
        double indexScanCost = calculateIndexScanCost(tableName, columnName, indexConditionsCount, conditionsCount);
        double y = (seqScanCost / indexScanCost);
        Pair<Long, Long> result = getTablePagesAndRowsCount(tableName);
        double numPages = result.getLeft();
        double numTuples = result.getRight();

        double bitmapScanCost = calculateBitmapHeapAndIndexScanCost(tableName, columnName,
                indexConditionsCount, conditionsCount, 1);
        double x = (indexScanCost - (bitmapScanCost - numPages * seqPageCost)) / (numPages * seqPageCost);
        double maxSel = Math.min(x, y);
        return (long) (maxSel * numTuples);
    }

    public static Pair<Long, Long> calculateBitmapIndexScanTuplesRange(String tableName, String indexedColumn,
                                                                       int indexConditionsCount, int conditionsCount) {
        double indexScanCost, numPages, numTuples, bitmapIdxScanCost, seqScanCost, bitmapScanCost;

        bitmapScanCost = calculateBitmapHeapAndIndexScanCost(tableName, indexedColumn,
                indexConditionsCount, conditionsCount, 1);
        seqScanCost = calculateSeqScanCost(tableName, conditionsCount + indexConditionsCount);
        indexScanCost = calculateIndexScanCost(tableName, indexedColumn, indexConditionsCount, conditionsCount);

        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);
        numPages = resultTable.getLeft();
        numTuples = resultTable.getRight();

        // HERE GOES MATH :)

        double maxSel = (seqScanCost - seqPageCost * numPages) / (bitmapScanCost - seqPageCost * numPages);
        double minSel = (indexScanCost - (bitmapScanCost - numPages * seqPageCost)) / (numPages * seqPageCost);
        if (minSel > 1) {
            return new ImmutablePair<>(3L, (long) ((maxSel - 0.05) * numTuples));
        }
        return new ImmutablePair<>((long) (minSel  * numTuples), (long) ((maxSel - 0.05) * numTuples));
    }

    //Helpful Functions

    private static double getInaccurateBits(Long numTuples) {
        return (double) (Math.max(0, (numTuples - getWorkMem()))) / numTuples;
    }
}

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
        return calculateIndexScanCost(tableName, indexedColumn, indexConditionsCount, conditionsCount, 1);
    }

    public static double calculateIndexScanCost(String tableName, String indexedColumn,
                                                int indexConditionsCount, int conditionsCount, double sel) {
        int visiblePages = getVisiblePages(tableName);
        double idxCost, tblCost, numIndexTuples, numIndexPages, numTuples, numPages,
                startup, runCost, idxCpuCost, tableCpuCost, indexIOCost, tableIOCost,
                maxIOCost, minIOCost, height;

        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);
        numPages = resultTable.getLeft();
        numTuples = resultTable.getRight();
        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numIndexPages = resultIndexTable.getLeft();
        numIndexTuples = resultIndexTable.getRight();

        height = Math.ceil(Math.log(numIndexPages) / Math.log(2));

        startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (height + 1) * 50) * cpuOperatorCost;

        idxCpuCost = sel * numIndexTuples * (cpuIndexTupleCost + qualOpCost * indexConditionsCount);
        tableCpuCost = sel * numTuples * cpuTupleCost + cpuOperatorCost * numTuples * conditionsCount;
        indexIOCost = Math.ceil(sel * numIndexPages) * randomPageCost;

        maxIOCost = numPages  * randomPageCost;
        minIOCost = randomPageCost + (Math.ceil(sel * numPages) - 1) * seqPageCost;

        tableIOCost = maxIOCost +
                Math.pow(getCorrelation(tableName, indexedColumn), 2) * (minIOCost - maxIOCost);
        runCost = idxCpuCost + tableCpuCost + indexIOCost + tableIOCost;

        return startup + runCost;
    }

    public static double calculateIndexOnlyScanCost(String tableName, String indexedColumn,
                                                    int indexConditionsCount, int conditionsCount) {
        return calculateIndexOnlyScanCost(tableName, indexedColumn, indexConditionsCount, conditionsCount, 1);
    }

    public static double calculateIndexOnlyScanCost(String tableName, String indexedColumn,
                                                    int indexConditionsCount, int conditionsCount, double sel) {
        int visiblePages = getVisiblePages(tableName);
        double idxCost, tblCost, numIndexTuples, numIndexPages, numTuples, numPages,
                startup, runCost, idxCpuCost, tableCpuCost, indexIOCost, tableIOCost,
                maxIOCost, minIOCost, height, fracVisiblePages, idxCostPerTuple;

        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);
        numPages = resultTable.getLeft();
        numTuples = resultTable.getRight();
        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numIndexPages = resultIndexTable.getLeft();
        numIndexTuples = resultIndexTable.getRight();

        height = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));

        startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (height + 1) * 50) * cpuOperatorCost;

        idxCostPerTuple = cpuIndexTupleCost + qualOpCost * indexConditionsCount;

        idxCpuCost = sel * numIndexTuples * (idxCostPerTuple);
        tableCpuCost = sel * numTuples * cpuTupleCost;
        indexIOCost = Math.ceil(sel * numIndexPages) * randomPageCost;

        fracVisiblePages = (1 - getVisiblePages(tableName)/ numPages);
        maxIOCost = fracVisiblePages * numPages  * randomPageCost;
        if (maxIOCost > 0) {
            minIOCost = randomPageCost + (Math.ceil(sel * numPages) - 1) * seqPageCost;
        } else {
            minIOCost = 0;
        }

        tableIOCost = maxIOCost +
                Math.pow(getCorrelation(tableName, indexedColumn), 2) * (minIOCost - maxIOCost);
        runCost = idxCpuCost + indexIOCost + tableCpuCost + tableIOCost;

        return startup + runCost;
    }

    public static double calculateBitmapIndexScanCost(String tableName, String indexedColumn,
                                                      int indexConditionsCount) {
        double idxCost, numPages, numTuples;

        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numPages = resultIndexTable.getLeft();
        numTuples = resultIndexTable.getRight();

        // WARNING: we're guessing indexConditionsCount = indexedColumns * 2 because we are using 2
        // conditions: less(<) and greater(>) for each column.

        idxCost = (randomPageCost * numPages) + (cpuIndexTupleCost * numTuples) +
                (cpuOperatorCost * indexConditionsCount * numTuples);
        idxCost *= (double)(indexConditionsCount / 2);

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
        if (pagesFetched == 1) {
            costPerPage = randomPageCost;
        }
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
        return (Long) (long) (((seqScanCost / indexOnlyScanCost) - 0.1) * getTableRowCount(tableName));
    }

    public static Long calculateIndexScanMaxTuples(String tableName, String columnName,
                                                   int conditionsCount, int indexConditionsCount) {
        double seqScanCost = calculateSeqScanCost(tableName, conditionsCount + indexConditionsCount);
        double indexScanCost = calculateIndexScanCost(tableName, columnName, indexConditionsCount, conditionsCount);
        double y = (seqScanCost / (indexScanCost));
        Pair<Long, Long> result = getTablePagesAndRowsCount(tableName);
        double numPages = result.getLeft();
        double numTuples = result.getRight();

        double bitmapScanCost = calculateBitmapHeapAndIndexScanCost(tableName, columnName,
                indexConditionsCount, conditionsCount, 1);
        double x = (indexScanCost - (bitmapScanCost - numPages * seqPageCost)) / (numPages * seqPageCost);
        double maxSel = Math.min(x, y);
        if (indexScanCost > bitmapScanCost) {
            System.out.println();
        }
        if (x > 1) {
            x = 1 / x;
        }
        if (maxSel < 0) {
            maxSel = y;
        }
        maxSel -= 0.005;
        double m1 = calculateBitmapHeapAndIndexScanCost(tableName, columnName, indexConditionsCount, conditionsCount, x * y);
        double m2 = indexScanCost * x * y;
        return (long) Math.max((x * y * numTuples), 2);
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

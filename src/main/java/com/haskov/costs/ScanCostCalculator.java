package com.haskov.costs;

import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;

import static com.haskov.costs.CostParameters.*;
import static com.haskov.utils.SQLUtils.*;

//TODO optimize class
public class ScanCostCalculator {
    private static final Map<ScanData, Long> maxTuplesMap = new HashMap<>();

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
        double numIndexTuples, numIndexPages, numTuples, numPages,
                startup, runCost, idxCpuCost, tableCpuCost, indexIOCost, tableIOCost,
                maxIOCost, minIOCost, height, idxCostPerTuple;

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
        tableCpuCost = sel * numTuples * (cpuTupleCost + conditionsCount * cpuOperatorCost);
        indexIOCost = Math.ceil(sel * numIndexPages) * randomPageCost;

        maxIOCost = numPages  * randomPageCost;
        minIOCost = randomPageCost + (Math.ceil(sel * numPages) - 1) * seqPageCost;

        tableIOCost = maxIOCost +
                Math.pow(getCorrelation(tableName, indexedColumn), 2) * (minIOCost - maxIOCost);
        runCost = idxCpuCost + indexIOCost + tableCpuCost + tableIOCost;

        return (double) Math.round(startup * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    public static double calculateIndexOnlyScanCost(String tableName, String indexedColumn,
                                                    int indexConditionsCount, int conditionsCount) {
        return calculateIndexOnlyScanCost(tableName, indexedColumn, indexConditionsCount, conditionsCount, 1);
    }

    public static double calculateIndexOnlyScanCost(String tableName, String indexedColumn,
                                                    int indexConditionsCount, int conditionsCount, double sel) {
        double numIndexTuples, numIndexPages, numTuples, numPages,
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

        return (double) Math.round(startup * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    public static double calculateBitmapIndexScanCost(String tableName, String indexedColumn,
                                                      int indexConditionsCount, double sel) {

        double numIndexTuples, numIndexPages,
                startup, runCost, idxCpuCost, indexIOCost,
                height, idxCostPerTuple;
        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numIndexPages = resultIndexTable.getLeft();
        numIndexTuples = resultIndexTable.getRight();

        height = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));

        startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (height + 1) * 50) * cpuOperatorCost;

        idxCostPerTuple = cpuIndexTupleCost + qualOpCost * indexConditionsCount;

        idxCpuCost = sel * numIndexTuples * (idxCostPerTuple);
        indexIOCost = Math.ceil(sel * numIndexPages) * randomPageCost;
        runCost = idxCpuCost + indexIOCost;
        return (double) Math.round(startup * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    public static double calculateBitmapHeapAndIndexScanCost(String tableName, String indexedColumn,
                                                             int indexConditionsCount, int conditionsCount,
                                                             double sel) {
        double idxCost = calculateBitmapIndexScanCost(tableName, indexedColumn, indexConditionsCount, sel);
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
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }



    // Calculate max tuples


    public Long calculateIndexOnlyScanMaxTuples(String tableName, String columnName,
                                                       int indexConditionsCount, int conditionsCount) {
        String scanType = "IndexOnlyScan";
        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);
        double numPages = resultTable.getLeft();
        double numTuples = resultTable.getRight();
        ScanData data = new ScanData(indexConditionsCount, conditionsCount, scanType,
                numPages, numTuples);
        if (maxTuplesMap.containsKey(data)) {
            return maxTuplesMap.get(data);
        }
        double seqScanCost = calculateSeqScanCost(tableName, conditionsCount + indexConditionsCount);

        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, columnName));
        double numIndexTuples = resultIndexTable.getRight();

        double height = getBtreeHeight(getIndexOnColumn(tableName, columnName));
        double startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (height + 1) * 50) * cpuOperatorCost;

        double indexOnlyScanCost = calculateIndexOnlyScanCost(tableName, columnName, indexConditionsCount, conditionsCount);
        long maxTuples = (long) ((((seqScanCost - randomPageCost + seqPageCost - startup) / indexOnlyScanCost) - 0.05)
                * getTableRowCount(tableName));
        maxTuplesMap.put(data, maxTuples);
        return maxTuples;
    }


    public Long calculateIndexScanMaxTuples(String tableName, String columnName,
                                                   int conditionsCount, int indexConditionsCount) {
        String scanType = "IndexScan";
        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);
        double numPages = resultTable.getLeft();
        double numTuples = resultTable.getRight();

        ScanData data = new ScanData(indexConditionsCount, conditionsCount, scanType,
                numPages, numTuples);
        if (maxTuplesMap.containsKey(data)) {
            return maxTuplesMap.get(data);
        }
        double seqScanCost = calculateSeqScanCost(tableName, conditionsCount + indexConditionsCount);
        double indexScanCost = calculateIndexScanCost(tableName, columnName, indexConditionsCount, conditionsCount);
        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, columnName));
        double numIndexTuples = resultIndexTable.getRight();

        double height = getBtreeHeight(getIndexOnColumn(tableName, columnName));
        double startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (height + 1) * 50) * cpuOperatorCost;


        double sel = (seqScanCost - randomPageCost + seqPageCost - startup) / indexScanCost - 0.05;

        double bitmapScanCost = calculateBitmapHeapAndIndexScanCost(tableName, columnName, indexConditionsCount,
                conditionsCount, sel);
        indexScanCost = calculateIndexScanCost(tableName, columnName, indexConditionsCount, conditionsCount, sel);

        double sel2 = sel;
        while (indexScanCost >= bitmapScanCost - 0.05 * bitmapScanCost) {
            if (sel * numTuples <= 1) {
                return 2L;
            }
            sel2 = 0.5;
            sel *= sel2;
            indexScanCost = calculateIndexScanCost(tableName, columnName, indexConditionsCount, conditionsCount,
                    sel);
            bitmapScanCost = calculateBitmapHeapAndIndexScanCost(tableName, columnName,
                    indexConditionsCount, conditionsCount, sel);

        }

        long maxTuples = (long) ((sel - 0.05) * numTuples);
        maxTuplesMap.put(data, maxTuples);
        return maxTuples;
    }

    public Pair<Long, Long> calculateBitmapIndexScanTuplesRange(String tableName, String indexedColumn,
                                                                       int indexConditionsCount, int conditionsCount) {
        String scanType = "BitmapScan";
        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);
        double numPages = resultTable.getLeft();
        double numTuples = resultTable.getRight();

        ScanData data = new ScanData(indexConditionsCount, conditionsCount, scanType,
                numPages, numTuples);
        if (maxTuplesMap.containsKey(data)) {
            return new ImmutablePair<>(3L, maxTuplesMap.get(data));
        }

        double seqScanCost = calculateSeqScanCost(tableName, conditionsCount + indexConditionsCount);
        double bitmapScanCost = calculateBitmapHeapAndIndexScanCost(tableName, indexedColumn, indexConditionsCount,
                conditionsCount, 1);

        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        double numIndexTuples = resultIndexTable.getRight();

        double height = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));
        double startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (height + 1) * 50) * cpuOperatorCost;

        double maxSel = (seqScanCost - randomPageCost + seqPageCost - startup - seqPageCost * numPages)
                / bitmapScanCost - 0.05;

        long maxTuples = (long)((maxSel) * numTuples);
        maxTuplesMap.put(data, maxTuples);
        return new ImmutablePair<>(3L, maxTuples);
    }

    //Helpful Functions

    private static double getInaccurateBits(Long numTuples) {
        return (double) (Math.max(0, (numTuples - getWorkMem()))) / numTuples;
    }

    public static double getIndexScanStartUpCost(String tableName, String indexedColumn) {
        double numIndexTuples, numIndexPages, height, startup;
        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numIndexPages = resultIndexTable.getLeft();
        numIndexTuples = resultIndexTable.getRight();

        height = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));

        startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (height + 1) * 50) * cpuOperatorCost;
        return startup;
    }
}

package com.haskov.costs;

import com.haskov.types.ScanNodeType;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.haskov.costs.CostParameters.*;
import static com.haskov.utils.SQLUtils.*;


public class ScanCostCalculator {
    private final Map<ScanCacheData, Pair<Long, Long>> cacheMap = new HashMap<>();


    //Seq Scan
    public double calculateSeqScanCost(String tableName, int conditionCount) {

        Pair<Long, Long> result = SQLUtils.getTablePagesAndRowsCount(tableName);
        double numPages = result.getLeft();
        double numTuples = result.getRight();
        return getSeqScanCost(numPages, numTuples, conditionCount);
    }

    private double getSeqScanCost(double numPages, double numTuples, int conditionCount) {
        return (seqPageCost * numPages) + (cpuTupleCost * numTuples) + (cpuOperatorCost * conditionCount * numTuples);
    }

    //Index Scan

    public double calculateIndexScanCost(String tableName, String indexedColumn,
                                                    int indexConditionsCount, int conditionsCount) {
        return calculateIndexScanCost(tableName, indexedColumn, indexConditionsCount, conditionsCount, 1);
    }

    public double calculateIndexScanCost(String tableName, String indexedColumn,
                                                int indexConditionsCount, int conditionsCount, double sel) {
        double numIndexTuples, numIndexPages, numTuples, numPages, correlation, height;

        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);
        numPages = resultTable.getLeft();
        numTuples = resultTable.getRight();

        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numIndexPages = resultIndexTable.getLeft();
        numIndexTuples = resultIndexTable.getRight();

        height = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));
        correlation = getCorrelation(tableName, indexedColumn);

        return getIndexScanCost(numPages, numTuples, numIndexPages, numIndexTuples,
                indexConditionsCount, conditionsCount,
                sel, height, correlation);
    }

    private double getIndexScanCost(double numPages, double numTuples,
                                           double numIndexPages, double numIndexTuples,
                                           int indexConditionsCount, int conditionsCount, double sel,
                                           double bTreeHeight, double correlation) {
        double startup, runCost, idxCpuCost, tableCpuCost, indexIOCost, tableIOCost,
                maxIOCost, minIOCost, idxCostPerTuple;

        startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (bTreeHeight + 1) * 50) * cpuOperatorCost;

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

    // Index Only Scan

    public double calculateIndexOnlyScanCost(String tableName, String indexedColumn,
                                                    int indexConditionsCount, int conditionsCount) {
        return calculateIndexOnlyScanCost(tableName, indexedColumn, indexConditionsCount, conditionsCount, 1);
    }

    public double calculateIndexOnlyScanCost(String tableName, String indexedColumn,
                                                    int indexConditionsCount, int conditionsCount, double sel) {
        double numIndexTuples, numIndexPages, numTuples, numPages,
                 height, visiblePages, correlation;

        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);
        numPages = resultTable.getLeft();
        numTuples = resultTable.getRight();

        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numIndexPages = resultIndexTable.getLeft();
        numIndexTuples = resultIndexTable.getRight();

        height = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));
        visiblePages = getVisiblePages(tableName);
        correlation = getCorrelation(tableName, indexedColumn);

        return getIndexOnlyScanCost(numPages, numTuples, numIndexPages, numIndexTuples,
                indexConditionsCount, conditionsCount, sel,
                height, correlation, visiblePages);
    }

    private double getIndexOnlyScanCost(double numPages, double numTuples,
                                              double numIndexPages, double numIndexTuples,
                                              int indexConditionsCount, int conditionsCount, double sel,
                                              double bTreeHeight, double correlation, double visiblePages) {
        double startup, runCost, idxCpuCost, tableCpuCost, indexIOCost, tableIOCost,
                maxIOCost, minIOCost, fracVisiblePages, idxCostPerTuple;

        startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (bTreeHeight + 1) * 50) * cpuOperatorCost;

        idxCostPerTuple = cpuIndexTupleCost + qualOpCost * indexConditionsCount;

        idxCpuCost = sel * numIndexTuples * (idxCostPerTuple);
        tableCpuCost = sel * numTuples * cpuTupleCost;
        indexIOCost = Math.ceil(sel * numIndexPages) * randomPageCost;

        fracVisiblePages = (1 - visiblePages/ numPages);
        maxIOCost = fracVisiblePages * numPages  * randomPageCost;
        if (maxIOCost > 0) {
            minIOCost = randomPageCost + (Math.ceil(sel * numPages) - 1) * seqPageCost;
        } else {
            minIOCost = 0;
        }

        tableIOCost = maxIOCost +
                Math.pow(correlation, 2) * (minIOCost - maxIOCost);
        runCost = idxCpuCost + indexIOCost + tableCpuCost + tableIOCost;

        return (double) Math.round(startup * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    //Bitmap Scan

    public double calculateBitmapIndexScanCost(String tableName, String indexedColumn,
                                                      int indexConditionsCount, double sel) {

        double numIndexTuples, numIndexPages,
                height;
        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        numIndexPages = resultIndexTable.getLeft();
        numIndexTuples = resultIndexTable.getRight();

        height = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));

        return getBitmapIndexScanCost(numIndexPages, numIndexTuples, indexConditionsCount, sel,
                height);
    }

    public double calculateBitmapHeapAndIndexScanCost(String tableName, String indexedColumn,
                                                             int indexConditionsCount, int conditionsCount,
                                                             double sel) {

        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);

        double numPages = resultTable.getLeft();
        double numTuples = resultTable.getRight();

        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        double numIndexPages = resultIndexTable.getLeft();
        double numIndexTuples = resultIndexTable.getRight();

        double height = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));

        return getBitmapHeapAndIndexScanCost(numPages, numTuples, numIndexPages, numIndexTuples,
                indexConditionsCount, conditionsCount, sel, height);
    }

    private double getBitmapIndexScanCost(double numIndexPages, double numIndexTuples,
                                                 int indexConditionsCount, double sel,
                                                 double bTreeHeight) {
        double startup, runCost, idxCpuCost, indexIOCost, idxCostPerTuple;

        startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (bTreeHeight + 1) * 50) * cpuOperatorCost;
        //startup = 0;

        idxCostPerTuple = cpuIndexTupleCost + qualOpCost * indexConditionsCount;

        idxCpuCost = sel * numIndexTuples * (idxCostPerTuple);
        indexIOCost = Math.ceil(sel * numIndexPages) * randomPageCost;
        runCost = idxCpuCost + indexIOCost;
        return (double) Math.round(startup * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    private double getBitmapHeapAndIndexScanCost(double numPages, double numTuples,
                                                        double numIndexPages, double numIndexTuples,
                                                        int indexConditionsCount, int conditionsCount, double sel,
                                                        double bTreeHeight) {

        double idxCost = getBitmapIndexScanCost(numIndexPages, numIndexTuples, indexConditionsCount,
                sel, bTreeHeight);

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

    // Calculate max tuples

    public Pair<Long, Long> calculateTuplesRange(String tableName, String indexedColumn,
                                              int indexConditionsCount, int conditionsCount, ScanNodeType type) {
        Pair<Long, Long> resultTable = getTablePagesAndRowsCount(tableName);
        double numPages = resultTable.getLeft();
        double numTuples = resultTable.getRight();

        ScanCacheData data = new ScanCacheData(indexConditionsCount, conditionsCount, type,
                numPages, numTuples);
        if (cacheMap.containsKey(data)) {
            return new ImmutablePair<>(cacheMap.get(data).getLeft(), cacheMap.get(data).getRight());
        }

        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        double numIndexPages = resultIndexTable.getLeft();
        double numIndexTuples = resultIndexTable.getRight();

        double btreeHeight = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));
        double correlation = getCorrelation(tableName, indexedColumn);
        double visiblePages = getVisiblePages(tableName);

        List<Pair<ScanNodeType, Long>> rangeList = new ArrayList<>();

        rangeList.add(new ImmutablePair<>(ScanNodeType.INDEX_SCAN, 1L));

        for (int i = 2; i <= numTuples; i++) {
            double sel = 1 / numTuples;
            List<ScanNodeType> types = new ArrayList<>(List.of(
                    ScanNodeType.SEQ_SCAN,
                    ScanNodeType.INDEX_SCAN,
                    ScanNodeType.INDEX_ONLY_SCAN,
                    ScanNodeType.BITMAP_SCAN)
            );
            List<Double> costs = new ArrayList<>(List.of(
                    getSeqScanCost(numPages, numTuples, conditionsCount),
                    getIndexScanCost(numPages, numTuples, numIndexPages, numIndexTuples,
                            indexConditionsCount, conditionsCount, sel, btreeHeight, correlation),
                    getIndexOnlyScanCost(numPages, numTuples, numIndexPages, numIndexTuples,
                            indexConditionsCount, conditionsCount, sel, btreeHeight, correlation, visiblePages),
                    getBitmapHeapAndIndexScanCost(numPages, numTuples, numIndexPages, numIndexTuples,
                            indexConditionsCount, conditionsCount, sel, btreeHeight)
            ));

            ScanNodeType bestType = types.stream()
                    .min(Comparator.comparingDouble(t -> costs.get(types.indexOf(t))))
                    .orElse(ScanNodeType.SEQ_SCAN);

            rangeList.add(new ImmutablePair<>(bestType, (long) i));
        }

        List<Long> costList = rangeList.stream()
                .filter(pair -> pair.getKey().equals(type))
                .map(Pair::getValue)
                .toList();

        Pair<Long, Long> range = new ImmutablePair<>(costList.getFirst(), costList.getLast());
        cacheMap.put(data, range);
        return range;
    }

    //Helpful Functions

    public double getIndexScanStartUpCost(String tableName, String indexedColumn) {
        double numIndexTuples, numIndexPages, height, startup;
        Pair<Long, Long> resultIndexTable = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));

        numIndexPages = resultIndexTable.getLeft();
        numIndexTuples = resultIndexTable.getRight();

        height = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));

        startup = Math.ceil(Math.log(numIndexTuples)/Math.log(2) + (height + 1) * 50) * cpuOperatorCost;
        return startup;
    }
}

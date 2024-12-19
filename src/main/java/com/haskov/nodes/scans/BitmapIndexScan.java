package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.ScanCostCalculator;
import com.haskov.nodes.Node;
import com.haskov.types.InsertType;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.haskov.tables.TableBuilder.buildRandomTable;


public class BitmapIndexScan implements Node, Scan {
    private List<String> nonIndexColumns = new ArrayList<>();
    private List<String> indexColumns = new ArrayList<>();
    private int nonIndexColumnsCount = 0;
    private int indexColumnsCount = 0;
    private String indexColumn = "";
    private String table = "";
    private final ScanCostCalculator costCalculator = new ScanCostCalculator();
    private long sel = 0;

    @Override
    public TableBuildResult initScanNode(Long tableSize) {
        TableBuildResult result = createTable(tableSize);
        table = result.tableName();
        Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(table);
        String[] columns = columnsAndTypes.keySet().toArray(new String[0]);
        for (String column : columns) {
            if (SQLUtils.hasIndexOnColumn(table, column)) {
                indexColumns.add(column);
            }
            else {
                nonIndexColumns.add(column);
            }
        }
        return result;
    }

    @Override
    public long reCalculateMinTuple(long tuples) {
        return tuples;
    }

    @Override
    public void prepareQuery() {
        Random random = new Random();
        nonIndexColumnsCount = random.nextInt(nonIndexColumns.size()) + 1;
        Collections.shuffle(indexColumns);
        Collections.shuffle(nonIndexColumns);
    }

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        Random random = new Random();

        indexColumnsCount = 1;

        qb.from(table);
        qb.setIndexConditionCount((indexColumnsCount)*2);
        qb.setConditionCount((nonIndexColumnsCount)*2);

        for (int j = 0; j < indexColumnsCount; j++) {
            qb.randomWhere(table, indexColumns.get(j));
            indexColumn = indexColumns.get(j);
        }
        for (int j = 0; j < nonIndexColumnsCount; j++) {
            qb.randomWhere(table, nonIndexColumns.get(j));
        }

        return qb;
    }


    @Override
    public TableBuildResult createTable(Long tableSize) {
        String tableName = "pg_bitmapscan";
        return buildRandomTable(tableName, tableSize, InsertType.RANDOM);
    }

    @Override
    public Pair<Double, Double> getCosts() {
        long maxTuples = costCalculator.calculateBitmapIndexScanTuplesRange
                (table, indexColumn, indexColumnsCount * 2, 0).getRight();
        sel = maxTuples / SQLUtils.getTableRowCount(table);
        double startUpCost = ScanCostCalculator.getIndexScanStartUpCost
                (table, indexColumn);
        double totalCost = ScanCostCalculator.calculateIndexOnlyScanCost
                (table, indexColumn, indexColumnsCount * 2,
                        nonIndexColumnsCount * 2, sel);
        return new ImmutablePair<>(startUpCost, totalCost);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return new ImmutablePair<>(indexColumnsCount * 2, nonIndexColumnsCount * 2);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        Pair<Long, Long> range = costCalculator.calculateBitmapIndexScanTuplesRange
                (table, indexColumn, indexColumnsCount * 2, nonIndexColumnsCount * 2);
        long minTuples = range.getLeft();
        long maxTuples = range.getRight();
        sel = maxTuples / SQLUtils.getTableRowCount(table);
        return new ImmutablePair<>(minTuples, maxTuples);
    }

    @Override
    public double getSel() {
        return sel;
    }

    @Override
    public String buildQuery() {
        return "";
    }
}

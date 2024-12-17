package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.ScanCostCalculator;
import com.haskov.nodes.Node;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.haskov.tables.TableBuilder.buildRandomTable;
import static com.haskov.utils.SQLUtils.hasIndexOnColumn;


public class IndexOnlyScan implements Node, Scan {
    private List<String> indexColumns = new ArrayList<>();
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
        }
        indexColumnsCount = 1;
        return result;
    }

    @Override
    public void prepareQuery() {
        Collections.shuffle(indexColumns);
    }

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {

        qb.setIndexConditionCount(indexColumnsCount * 2);
        qb.from(table);

        for (String column : indexColumns) {
            qb.randomWhere(table, column);
            indexColumn = column;
            break;
        }

        return qb;
    }

    @Override
    public TableBuildResult createTable(Long tableSize) {
        String tableName = "pg_indexonlyscan";
        return buildRandomTable(tableName, tableSize);
    }

    @Override
    public Pair<Double, Double> getCosts() {
        long maxTuples = costCalculator.calculateIndexOnlyScanMaxTuples
                (table, indexColumn, 0, indexColumnsCount * 2);
        sel = maxTuples / SQLUtils.getTableRowCount(table);
        double startUpCost = ScanCostCalculator.getIndexScanStartUpCost
                (table, indexColumn);
        double totalCost = ScanCostCalculator.calculateIndexOnlyScanCost
                (table, indexColumn, 0, indexColumnsCount * 2, sel);
        return new ImmutablePair<>(startUpCost, totalCost);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return new ImmutablePair<>(indexColumnsCount * 2, 0);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        long minTuples = 0;
        long maxTuples = costCalculator.calculateIndexOnlyScanMaxTuples
                (table, indexColumn, 0, indexColumnsCount * 2);
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

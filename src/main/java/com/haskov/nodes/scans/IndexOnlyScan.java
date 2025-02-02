package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.ScanCostCalculator;
import com.haskov.nodes.Node;
import com.haskov.types.*;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.haskov.tables.TableBuilder.buildRandomTable;


public class IndexOnlyScan implements Node, Scan {
    private List<String> indexColumns = new ArrayList<>();
    private int indexColumnsCount = 0;
    private String indexColumn = "";
    private String table = "";
    private final ScanCostCalculator costCalculator = new ScanCostCalculator();
    private long sel = 0;
    private long tableSize;

    @Override
    public TableBuildResult initScanNode(Long tableSize) {
        TableBuildResult result = createTable(tableSize);
        table = result.tableName();
        this.tableSize = tableSize;
        Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(table);
        String[] columns = columnsAndTypes.keySet().toArray(new String[0]);
        indexColumns = new ArrayList<>(Arrays.asList(columns));
        indexColumnsCount = 1;
        return result;
    }

    @Override
    public void prepareScanQuery() {
        Collections.shuffle(indexColumns);
        indexColumn = indexColumns.getFirst();
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
        return buildRandomTable(tableName, tableSize,
                InsertType.ASCENDING, TableIndexType.FULL_UNIQUE_INDEX);
    }

    @Override
    public Pair<Double, Double> getCosts(double sel) {
        Pair<Long, Long> range = costCalculator.calculateTuplesRange
                (table, indexColumn, indexColumnsCount * 2, 0,
                        ScanNodeType.INDEX_ONLY_SCAN);
        double startUpCost = costCalculator.getIndexScanStartUpCost
                (table, indexColumn);
        double totalCost = costCalculator.calculateIndexOnlyScanCost
                (table, indexColumn, 0, indexColumnsCount * 2, sel);
        return new ImmutablePair<>(startUpCost, totalCost);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return new ImmutablePair<>(indexColumnsCount, 0);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        Pair<Long, Long> range = costCalculator.calculateTuplesRange
                (table, indexColumn, indexColumnsCount * 2, 0,
                        ScanNodeType.INDEX_ONLY_SCAN);
        long maxTuples = range.getRight();
        long minTuples = range.getLeft();
        sel = maxTuples / tableSize;
        return new ImmutablePair<>(minTuples, maxTuples);
    }

    @Override
    public List<String> getTables() {
        return List.of(table);
    }

    @Override
    public double getSel() {
        return sel;
    }

}

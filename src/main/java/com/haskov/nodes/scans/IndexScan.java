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


public class IndexScan implements Node, Scan {
    private List<String> nonIndexColumns = new ArrayList<>();
    private List<String> indexColumns = new ArrayList<>();
    private int nonIndexColumnsCount = 0;
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
        double tmpSel = (double) tuples / tableSize;
        while (tableSize * Math.pow(tmpSel, nonIndexColumnsCount) * Math.pow(tmpSel, indexColumnsCount) < 2) {
            tmpSel *= 1.05;
        }
        return (long) (tableSize * tmpSel);
    }

    @Override
    public String buildQuery() {
        return buildQuery(new QueryBuilder()).build();
    }

    @Override
    public void prepareQuery() {
        Random random = new Random();
        nonIndexColumnsCount = random.nextInt(nonIndexColumns.size()) + 1;
        indexColumnsCount = 1;
        Collections.shuffle(nonIndexColumns);
        Collections.shuffle(indexColumns);
        indexColumn = indexColumns.getFirst();
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
        }
        for (int j = 0; j < nonIndexColumnsCount; j++) {
            qb.randomWhere(table, nonIndexColumns.get(j));
        }

        return qb;
    }

    @Override
    public TableBuildResult createTable(Long tableSize) {
        String tableName = "pg_indexscan";
        return buildRandomTable(tableName, tableSize);
    }

    @Override
    public Pair<Double, Double> getCosts() {
        long maxTuples = costCalculator.calculateIndexScanMaxTuples
                (table, indexColumn, nonIndexColumnsCount * 2,
                        indexColumnsCount * 2);
        sel = maxTuples / SQLUtils.getTableRowCount(table);
        double startUpCost = ScanCostCalculator.getIndexScanStartUpCost
                (table, indexColumn);
        double totalCost = ScanCostCalculator.calculateIndexScanCost
                (table, indexColumn, indexColumnsCount * 2,
                        nonIndexColumnsCount * 2, sel);
        return new ImmutablePair<>(startUpCost, totalCost);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return new ImmutablePair<>(nonIndexColumnsCount * 2, indexColumnsCount * 2);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        long minTuples = 0;
        long maxTuples = costCalculator.calculateIndexScanMaxTuples
                (table, indexColumn, nonIndexColumnsCount * 2, indexColumnsCount * 2);
        sel = maxTuples / SQLUtils.getTableRowCount(table);
        return new ImmutablePair<>(minTuples, maxTuples);
    }

    @Override
    public double getSel() {
        return sel;
    }
}

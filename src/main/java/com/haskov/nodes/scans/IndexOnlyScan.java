package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.ScanCostCalculator;
import com.haskov.nodes.Node;
import com.haskov.types.InsertType;
import com.haskov.types.TableBuildResult;
import com.haskov.types.TableIndexType;
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
        indexColumns.addAll(Arrays.asList(columns));
        indexColumnsCount = 1;
        return result;
    }

    @Override
    public long reCalculateMinTuple(long tuples) {
        double tmpSel = (double) tuples / tableSize;
        while (tableSize* Math.pow(tmpSel, indexColumnsCount) < 2) {
            tmpSel *= 1.05;
        }
        return (long) (tableSize * tmpSel);
    }

    @Override
    public void prepareQuery() {
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
                InsertType.ASCENDING, TableIndexType.FULL_INDEX);
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

package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.scan.IndexOnlyScanCostCalculator;
import com.haskov.costs.scan.ScanTupleRangeCalculator;
import com.haskov.nodes.Node;
import com.haskov.types.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.haskov.tables.TableBuilder.buildRandomTable;


public class IndexOnlyScan implements Node, TableScan {
    private List<String> indexColumns = new ArrayList<>();
    private int indexColumnsCount = 0;
    private String indexColumn = "";
    private String table = "";
    private ScanTupleRangeCalculator tupleCalculator;

    @Override
    public TableBuildResult initScanNode(Long tableSize) {
        TableBuildResult result = createTable(tableSize);
        table = result.tableName();
        Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(table);
        String[] columns = columnsAndTypes.keySet().toArray(new String[0]);
        indexColumns = new ArrayList<>(Arrays.asList(columns));
        indexColumnsCount = 1;

        tupleCalculator = new ScanTupleRangeCalculator(table, indexColumns.getFirst());
        return result;
    }

    @Override
    public void prepareScanQuery() {
        Collections.shuffle(indexColumns);
        indexColumn = indexColumns.getFirst();
    }

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        Pair<Long, Long> tupleRange = getTuplesRange();
        qb.setMinMaxTuples(tupleRange.getLeft(), tupleRange.getRight());
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
        IndexOnlyScanCostCalculator costCalculator = tupleCalculator.getIndexOnlyCalculator();
        double startUpCost = costCalculator.calculateStartUpCost();
        double totalCost = costCalculator.calculateCost(indexColumnsCount * 2, sel);
        return new ImmutablePair<>(startUpCost, totalCost);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return new ImmutablePair<>(indexColumnsCount, 0);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        Pair<Long, Long> range = tupleCalculator.calculateTuplesRange
                (indexColumnsCount * 2, 0,
                        ScanNodeType.INDEX_ONLY_SCAN);
        long maxTuples = range.getRight();
        long minTuples = range.getLeft();
        return new ImmutablePair<>(minTuples, maxTuples);
    }

    @Override
    public List<String> getTables() {
        return List.of(table);
    }

}

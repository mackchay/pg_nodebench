package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.scan.IndexScanCostCalculator;
import com.haskov.costs.scan.ScanTupleRangeCalculator;
import com.haskov.types.ScanNodeType;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.haskov.tables.TableBuilder.buildRandomTable;


public class IndexScan implements Scan {
    private List<String> nonIndexColumns = new ArrayList<>();
    private List<String> indexColumns = new ArrayList<>();
    private int nonIndexColumnsCount = 0;
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
        for (String column : columns) {
            if (SQLUtils.hasIndexOnColumn(table, column)) {
                indexColumns.add(column);
            }
            else {
                nonIndexColumns.add(column);
            }
        }

        tupleCalculator = new ScanTupleRangeCalculator(table, indexColumns.getFirst());
        return result;
    }

    @Override
    public void prepareScanQuery() {
        Random random = new Random();
        nonIndexColumnsCount = random.nextInt(nonIndexColumns.size()) + 1;
        indexColumnsCount = 1;
        Collections.shuffle(nonIndexColumns);
        Collections.shuffle(indexColumns);
        indexColumn = indexColumns.getFirst();
    }

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        Pair<Long, Long> tupleRange = getTuplesRange();
        qb.setMinMaxTuples(tupleRange.getLeft(), tupleRange.getRight());

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
    public Pair<Double, Double> getCosts(double sel) {
        IndexScanCostCalculator costCalculator = tupleCalculator.getIndexCalculator();
        double startUpCost = costCalculator.calculateStartUpCost();
        double totalCost = costCalculator.calculateCost
                (indexColumnsCount * 2,
                        nonIndexColumnsCount * 2, sel);
        return new ImmutablePair<>(startUpCost, totalCost);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return new ImmutablePair<>(nonIndexColumnsCount, indexColumnsCount);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        Pair<Long, Long> range = tupleCalculator.calculateTuplesRange
                (nonIndexColumnsCount * 2, indexColumnsCount * 2,
                        ScanNodeType.INDEX_SCAN);
        return new ImmutablePair<>(range.getLeft(), range.getRight());
    }

    @Override
    public List<String> getTables() {
        return List.of(table);
    }
}

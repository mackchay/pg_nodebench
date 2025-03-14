package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.costs.scan.SeqScanCostCalculator;
import com.haskov.types.InsertType;
import com.haskov.types.TableBuildResult;
import com.haskov.types.TableIndexType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.haskov.bench.V2.getColumnsAndTypes;
import static com.haskov.tables.TableBuilder.buildRandomTable;

public class SeqScan implements TableScan {
    private int columnsConditionsCount = 0;
    private String table = "";
    private List<String> columns = new ArrayList<>();
    private long tableSize;
    private SeqScanCostCalculator costCalculator;

    @Override
    public TableBuildResult initScanNode(Long tableSize) {
        TableBuildResult result = createTable(tableSize);
        table = result.tableName();
        this.tableSize = tableSize;
        columns = new ArrayList<>(getColumnsAndTypes(table).keySet());
        costCalculator = new SeqScanCostCalculator(table);
        return result;
    }

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        Pair<Long, Long> tupleRange = getTuplesRange();
        qb.setMinMaxTuples(tupleRange.getLeft(), tupleRange.getRight());
        qb.from(table);
        for (String column : columns.subList(0, columnsConditionsCount)) {
            qb.randomWhere(table, column);
        }
        return qb;
    }

    @Override
    public void prepareScanQuery() {
        Random random = new Random();
        columnsConditionsCount = random.nextInt(columns.size()) + 1;
        Collections.shuffle(columns);
    }

    public TableBuildResult createTable(Long tableSize) {
        String tableName = "pg_seqscan";
        return buildRandomTable(tableName, tableSize,
                InsertType.ASCENDING, TableIndexType.FULL_NON_INDEX);
    }

    @Override
    public Pair<Double, Double> getCosts(double sel) {
        double totalCost = costCalculator.calculateCost(columnsConditionsCount * 2);
        return new ImmutablePair<>(0.0, totalCost);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return new ImmutablePair<>(0, columnsConditionsCount);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        return new ImmutablePair<>(0L, tableSize);
    }

    @Override
    public List<String> getTables() {
        return List.of(table);
    }
}

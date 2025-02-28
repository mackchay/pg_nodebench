package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FunctionScan implements TableScan {
    private String tableName;
    private long tableSize;

    @Override
    public QueryBuilder buildQuery(QueryBuilder queryBuilder) {
        Random random = new Random();
        queryBuilder.select(String.valueOf(random.nextInt()));
        queryBuilder.from(tableName);
        return queryBuilder;
    }

    @Override
    public Pair<Double, Double> getCosts(double sel) {
        return new ImmutablePair<>(0.0, (double)tableSize / 100);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        return new ImmutablePair<>(0L, 0L);
    }

    @Override
    public List<String> getTables() {
        return List.of();
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return new ImmutablePair<>(0, 0);
    }

    @Override
    public TableBuildResult initScanNode(Long tableSize) {
        TableBuildResult tableBuildResult = createTable(tableSize);
        tableName = tableBuildResult.tableName();
        this.tableSize = tableSize;
        return tableBuildResult;
    }

    @Override
    public TableBuildResult createTable(Long tableSize) {
        String tableName = "generate_series(1, " + tableSize + ")";
        return new TableBuildResult(tableName, new ArrayList<>());
    }

    @Override
    public void prepareScanQuery() {

    }
}

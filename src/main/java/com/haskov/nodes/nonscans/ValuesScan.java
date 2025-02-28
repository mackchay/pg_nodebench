package com.haskov.nodes.nonscans;

import com.haskov.QueryBuilder;
import com.haskov.nodes.scans.TableScan;
import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ValuesScan implements TableScan {
    private List<Integer> values = new ArrayList<>();

    @Override
    public QueryBuilder buildQuery(QueryBuilder queryBuilder) {
        queryBuilder.setValues(values);
        queryBuilder.from("");
        queryBuilder.select("");
        return queryBuilder;
    }

    @Override
    public Pair<Double, Double> getCosts(double sel) {
        return new ImmutablePair<>(0.0, 0.01);
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
        values = IntStream.rangeClosed(1, 10_000)
                .boxed()
                .collect(Collectors.toList());
        return new TableBuildResult("", Collections.emptyList());
    }

    @Override
    public TableBuildResult createTable(Long tableSize) {
        return null;
    }

    @Override
    public void prepareScanQuery() {
        Collections.shuffle(values);
    }
}

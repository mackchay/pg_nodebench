package com.haskov.nodes.nonscans;

import com.haskov.QueryBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Random;

public class WorkTableScan implements NonTableScan {
    private String table = "CTESource";

    @Override
    public QueryBuilder buildQuery(QueryBuilder queryBuilder) {
        Random random = new Random();
        int iterations = random.nextInt(10) + 1;
        int steps = random.nextInt(5) + 1;
        queryBuilder.selectRecursiveCounter("counter", steps, iterations);
        queryBuilder.from(table);
        return queryBuilder;
    }

    @Override
    public Pair<Double, Double> getCosts(double sel) {
        return new ImmutablePair<>(0.0, 0.00);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        return new ImmutablePair<>(0L, 0L);
    }

    @Override
    public List<String> getTables() {
        return List.of(table);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return new ImmutablePair<>(0, 0);
    }
}

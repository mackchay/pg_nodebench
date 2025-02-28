package com.haskov.nodes.nonscans;

import com.haskov.QueryBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Random;

public class Result implements NonTableScan {

    @Override
    public QueryBuilder buildQuery(QueryBuilder queryBuilder) {
        Random random = new Random();
        queryBuilder.select(String.valueOf(random.nextInt()));
        queryBuilder.from("");
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
}

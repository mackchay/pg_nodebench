package com.haskov.nodes.unions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.InternalNode;
import com.haskov.nodes.Node;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class SetOp implements InternalNode {
    private Node child;

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        qb = child.buildQuery(qb);
        qb.replaceUnionAllWithIntersect();
        return qb;
    }


    @Override
    public Pair<Double, Double> getCosts(double sel) {
        return child.getCosts(sel);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        return new ImmutablePair<>(0L, Long.MAX_VALUE);
    }

    @Override
    public List<String> getTables() {
        return child.getTables();
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return child.getConditions();
    }

    @Override
    public void initInternalNode(List<Node> nodes) {
        child = nodes.getLast();
    }
}

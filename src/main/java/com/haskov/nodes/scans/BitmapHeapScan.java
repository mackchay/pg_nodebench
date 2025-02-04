package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.nodes.InternalNode;
import com.haskov.nodes.Node;
import com.haskov.types.TableBuildResult;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;


public class BitmapHeapScan implements InternalNode {
    private Node child;

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        return child.buildQuery(qb);
    }

    @Override
    public Pair<Double, Double> getCosts(double sel) {
        return child.getCosts(sel);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        return child.getTuplesRange();
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
        child = nodes.getFirst();
    }
}

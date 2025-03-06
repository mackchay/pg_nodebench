package com.haskov.nodes.subquery;

import com.haskov.QueryBuilder;
import com.haskov.nodes.InternalNode;
import com.haskov.nodes.Node;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class CTEScan implements InternalNode {
    private Node child;

    @Override
    public void initInternalNode(List<Node> nodes) {
        child = nodes.getFirst();
    }

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        qb = child.buildQuery(qb);
        if (qb.isGlobalCTERequired()) {
            qb.setGlobalCTERequired(false);
        } else {
            qb.setSubQueryCTESource("CTESource");
        }
        return qb;
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
}

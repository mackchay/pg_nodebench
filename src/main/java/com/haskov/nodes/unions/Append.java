package com.haskov.nodes.unions;

import com.haskov.QueryBuilder;
import com.haskov.nodes.InternalNode;
import com.haskov.nodes.Node;
import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class Append implements InternalNode {
    private Node child;


    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        QueryBuilder q = new QueryBuilder();
        q.unionAll(qb);
        return q;
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

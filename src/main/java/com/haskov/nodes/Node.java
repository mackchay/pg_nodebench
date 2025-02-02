package com.haskov.nodes;

import com.haskov.QueryBuilder;
import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public interface Node {

    public default QueryBuilder buildQuery(QueryBuilder qb) {
        return qb;
    }

    /**
     * @return startUpCost, totalCost
     */
    Pair<Double, Double> getCosts(double sel);

    Pair<Long, Long> getTuplesRange();

    List<String> getTables();

    /**
     * @return indexConditionsCount, nonIndexConditionsCount
     */
    Pair<Integer, Integer> getConditions();
}

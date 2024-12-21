package com.haskov.nodes.joins;


import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.Pair;

public interface Join {
    //TODO remove this useless fun.
    public TableBuildResult prepareJoinTable(String childName, String parentTable);

    Pair<Double, Double> getCosts();

    void prepareJoinQuery(double parentTableCost, double childTableCost,
                          double parentTableSel, double childTableSel,
                          int innerConditionsCount, int outerConditionsCount,
                          double startScanCost);

    /**
     * @return minTuples, maxTuples
     */
    Pair<Long, Long> getTuplesRange();
}

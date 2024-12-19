package com.haskov.nodes.scans;

import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.Pair;

public interface Scan {

    /**
     * Required to be called first
     */
    public TableBuildResult initScanNode(Long tableSize);

    long reCalculateMinTuple(long tuples);

    public TableBuildResult createTable(Long tableSize);

    /**
     * @return startUpCost, totalCost
     */
    Pair<Double, Double> getCosts();

    /**
     * @return indexConditionsCount, nonIndexConditionsCount
     */
    Pair<Integer, Integer> getConditions();
    /**
     * @return minTuples, maxTuples
     */
    Pair<Long, Long> getTuplesRange();

    double getSel();
}

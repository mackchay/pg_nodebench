package com.haskov.nodes.scans;

import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public interface Scan {

    /**
     * Required to be called first
     */
    public TableBuildResult initScanNode(Long tableSize);

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

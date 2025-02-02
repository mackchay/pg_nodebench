package com.haskov.nodes.scans;

import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.Pair;

public interface Scan {

    /**
     * Required to be called first
     */
    public TableBuildResult initScanNode(Long tableSize);


    /**
     * @param tableSize размер таблицы в строках
     * @return результат генерации таблицы: название таблицы и sql-скрипты,
     * которые были использованы для ее создания
     */
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

    public default void prepareScanQuery() {

    }
    /**
     * @return возвращает селективность
     */
    double getSel();
}

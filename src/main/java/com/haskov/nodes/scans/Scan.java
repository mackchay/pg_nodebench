package com.haskov.nodes.scans;

import com.haskov.nodes.Node;
import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.Pair;

public interface Scan extends Node {

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


    public default void prepareScanQuery() {

    }
    /**
     * @return возвращает селективность
     */
    double getSel();
}

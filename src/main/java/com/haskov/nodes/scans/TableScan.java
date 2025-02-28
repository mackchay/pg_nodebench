package com.haskov.nodes.scans;

import com.haskov.nodes.Node;
import com.haskov.types.TableBuildResult;

public interface TableScan extends Node {

    /**
     * Required to be called first
     */
    TableBuildResult initScanNode(Long tableSize);


    /**
     * @param tableSize размер таблицы в строках
     * @return результат генерации таблицы: название таблицы и sql-скрипты,
     * которые были использованы для ее создания
     */
    TableBuildResult createTable(Long tableSize);


    void prepareScanQuery();
}

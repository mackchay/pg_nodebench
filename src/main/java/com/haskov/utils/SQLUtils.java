package com.haskov.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.haskov.bench.V2.*;

public class SQLUtils {

    public static double calculateSeqScanCost(String tableName, int conditionCount) {
        Map<String, String> costParameters = getCostParameters();
        double pageCost = Double.parseDouble(costParameters.get("seq_page_cost"));
        double cpuTupleCost = Double.parseDouble(costParameters.get("cpu_tuple_cost"));
        double cpuOperatorCost = Double.parseDouble(costParameters.get("cpu_operator_cost"));

        String query = """
            SELECT relpages, reltuples
            FROM pg_class
            WHERE relname = ?
        """;
        List<List<String>> result = select(query, tableName);
        if (!result.isEmpty()) {
            double numPages = Double.parseDouble(result.getFirst().getFirst());
            double numTuples = Double.parseDouble(result.getFirst().getLast());
            return (pageCost * numPages) + (cpuTupleCost * numTuples) + (cpuOperatorCost * conditionCount * numTuples);
        }
        return 0;
    }

    public static double calculateIndexScanCost(String tableName, String indexedColumn, int conditionCount) {
        Map<String, String> costParameters = getCostParameters();
        double randomPageCost = Double.parseDouble(costParameters.get("random_page_cost"));
        double cpuIndexTupleCost = Double.parseDouble(costParameters.get("cpu_index_tuple_cost"));
        double cpuOperatorCost = Double.parseDouble(costParameters.get("cpu_operator_cost"));
        double seqPageCost = Double.parseDouble(costParameters.get("seq_page_cost"));
        double cpuTupleCost = Double.parseDouble(costParameters.get("cpu_tuple_cost"));
        double idxCost = 0, tblCost = 0;

        String query = """
            SELECT relpages, reltuples
            FROM pg_class
            WHERE relname = ?
        """;

        List<List<String>> resultIndex = select(query, getIndexOnColumn(tableName, indexedColumn));
        if (!resultIndex.isEmpty()) {
            double numPages = Double.parseDouble(resultIndex.getFirst().getFirst());
            double numTuples = Double.parseDouble(resultIndex.getFirst().getLast());
            idxCost = (randomPageCost * numPages) + (cpuIndexTupleCost * numTuples) +
                    (cpuOperatorCost * conditionCount * numTuples);
        }

        List<List<String>> resultTable = select(query, tableName);
        if (!resultTable.isEmpty()) {
            double numPages = Double.parseDouble(resultTable.getFirst().getFirst());
            double numTuples = Double.parseDouble(resultTable.getFirst().getLast());
            if (getCorrelation(tableName, indexedColumn) > 0.5) {
                tblCost = (seqPageCost * numPages) + (cpuTupleCost * numTuples);
            } else {
                tblCost = (randomPageCost * numPages) + (cpuTupleCost * numTuples);
            }
        }

        return idxCost + tblCost;
    }

    public static double calculateIndexOnlyScanCost(String tableName, String indexedColumn, int conditionCount) {
        Map<String, String> costParameters = getCostParameters();
        double randomPageCost = Double.parseDouble(costParameters.get("random_page_cost"));
        double cpuIndexTupleCost = Double.parseDouble(costParameters.get("cpu_index_tuple_cost"));
        double cpuOperatorCost = Double.parseDouble(costParameters.get("cpu_operator_cost"));
        double seqPageCost = Double.parseDouble(costParameters.get("seq_page_cost"));
        double cpuTupleCost = Double.parseDouble(costParameters.get("cpu_tuple_cost"));
        double visiblePages = getVisiblePages(tableName);
        double idxCost = 0, tblCost = 0;

        String query = """
            SELECT relpages, reltuples
            FROM pg_class
            WHERE relname = ?
        """;

        List<List<String>> resultIndex = select(query, getIndexOnColumn(tableName, indexedColumn));
        if (!resultIndex.isEmpty()) {
            double numPages = Double.parseDouble(resultIndex.getFirst().getFirst());
            double numTuples = Double.parseDouble(resultIndex.getFirst().getLast());
            idxCost = (randomPageCost * numPages) + (cpuIndexTupleCost * numTuples) +
                    (cpuOperatorCost * conditionCount * numTuples);
        }

        List<List<String>> resultTable = select(query, tableName);
        if (!resultTable.isEmpty()) {
            double numPages = Double.parseDouble(resultTable.getFirst().getFirst());
            double numTuples = Double.parseDouble(resultTable.getFirst().getLast());
            double fracVisiblePages = visiblePages / numPages;
            if (getCorrelation(tableName, indexedColumn) > 0.5) {
                tblCost = (1 - fracVisiblePages) * (seqPageCost * numPages) + (cpuTupleCost * numTuples);
            } else {
                tblCost = (1 - fracVisiblePages) * (randomPageCost * numPages) + (cpuTupleCost * numTuples);
            }
        }

        return idxCost + tblCost;
    }

    public static Long calculateIndexOnlyScanMaxTuples(String tableName, String columnName, int conditionCount) {
        double seqScanCost = calculateSeqScanCost(tableName, conditionCount);
        double indexOnlyScanCost = calculateIndexOnlyScanCost(tableName, columnName, conditionCount);
        return (Long) (long) ((seqScanCost / indexOnlyScanCost) * getTableRowCount(tableName) -
                0.01 * getTableRowCount(tableName));
    }

    public static Long calculateIndexScanMaxTuples(String tableName, String columnName, int conditionCount) {
        double seqScanCost = calculateSeqScanCost(tableName, conditionCount);
        double indexScanCost = calculateIndexScanCost(tableName, columnName, conditionCount);
        return (Long) (long) ((seqScanCost / indexScanCost) * getTableRowCount(tableName) -
                0.01 * getTableRowCount(tableName));
    }

    public static String findBTreeIndexOnColumn(String tableName, String columnName) {
        String query = """
            SELECT i.relname as index_name, a.attname as column_name, t.relname as table_name
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_attribute a ON a.attrelid = t.oid
            JOIN pg_am am ON i.relam = am.oid
            WHERE a.attnum = ANY(ix.indkey)
            AND t.relname = ?
            AND a.attname = ?
            AND am.amname = 'btree'
        """;

        List<String> result = selectColumn(query, tableName, columnName);
        if (!result.isEmpty()) {
            return result.getFirst();
        }
        return null;
    }


    /*

    Helpful functions.

     */

    public static String getMax(String tableName, String columnName) {
        String query = "select max(" + tableName + "." + columnName + ") from " + tableName;
        List<Object> result = selectColumn(query);
        return result.getFirst().toString();
    }

    public static String getMin(String tableName, String columnName) {
        String query = "select min(" + tableName + "." + columnName + ") from " + tableName;
        List<Object> result = selectColumn(query);
        return result.getFirst().toString();
    }

    public static Long getTableRowCount(String tableName) {
        String query = "SELECT COUNT(*) AS row_count FROM " + tableName;
        return selectOne(query);
    }

    public static boolean hasIndexOnColumn(String tableName, String columnName) {
        String query = """
                SELECT
                    indexname
                FROM
                    pg_indexes
                WHERE
                    tablename = ?
                AND
                    indexdef LIKE '%' || ? || '%'
                """;
        String result = selectOne(query, tableName, columnName);
        return result != null;
    }

    public static String getIndexOnColumn(String tableName, String columnName) {
        String query = """
                SELECT
                    indexname
                FROM
                    pg_indexes
                WHERE
                    tablename = ?
                AND
                    indexdef LIKE '%' || ? || '%'
                """;
        return selectOne(query, tableName, columnName);
    }


    public static Map<String, String> getCostParameters() {
        String query = """
            SELECT name, setting
            FROM pg_settings
            WHERE name IN ('seq_page_cost', 'random_page_cost', 'cpu_tuple_cost', 'cpu_index_tuple_cost', 'cpu_operator_cost')
        """;

        List<List<String>> result = select(query);
        Map<String, String> costParameters = new HashMap<>();
        for (int i = 0; i < result.size(); i ++) {
            costParameters.put(result.get(i).getFirst(), result.get(i).get(1));
        }
        return costParameters;
    }

    public static Double getCorrelation(String tableName, String columnName) {
        String query = """
                SELECT correlation
                FROM pg_stats WHERE tablename = ?
                AND attname = ?
                """;
        return (Double) (double) (float)selectOne(query, tableName, columnName);
    }

    public static Integer getVisiblePages(String tableName) {
        String query = """
                SELECT relallvisible
                FROM pg_class WHERE relname = ?
                """;
        return selectOne(query, tableName);
    }
}

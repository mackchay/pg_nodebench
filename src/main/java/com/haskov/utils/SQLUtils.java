package com.haskov.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.haskov.bench.V2.*;

public class SQLUtils {

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
        List<String> result = selectColumn(query, tableName, columnName);
        return !result.isEmpty();
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

    public static double calculateSeqScanCost(String tableName) {
        Map<String, String> costParameters = getCostParameters();
        double pageCost = Double.parseDouble(costParameters.get("seq_page_cost"));
        double cpuTupleCost = Double.parseDouble(costParameters.get("cpu_tuple_cost"));

        String query = """
            SELECT relpages, reltuples
            FROM pg_class
            WHERE relname = ?
        """;
        List<List<String>> result = select(query, tableName);
        if (!result.isEmpty()) {
            double numPages = Double.parseDouble(result.getFirst().getFirst());
            double numTuples = Double.parseDouble(result.getFirst().getLast());
            return (pageCost * numPages) + (cpuTupleCost * numTuples);
        }
        return 0;
    }

    public static double calculateIndexOnlyScanCost(String indexName) {
        Map<String, String> costParameters = getCostParameters();
        double indexPageCost = Double.parseDouble(costParameters.get("random_page_cost"));
        double cpuTupleCost = Double.parseDouble(costParameters.get("cpu_tuple_cost"));

        String query = """
            SELECT relpages, reltuples
            FROM pg_class
            WHERE relname = ?
        """;
        List<List<String>> result = select(query, indexName);
        if (!result.isEmpty()) {
            double numPages = Double.parseDouble(result.getFirst().getFirst());
            double numTuples = Double.parseDouble(result.getFirst().getLast());
            return (indexPageCost * numPages) + (cpuTupleCost * numTuples);
        }
        return 0;
    }

    public static Long calculateIndexScanMaxTuples(String tableName, String columnName) {
        Map<String, String> costParameters = getCostParameters();
        String indexName = findBTreeIndexOnColumn(tableName, columnName);
        double indexPageCost = Double.parseDouble(costParameters.get("random_page_cost"));
        double cpuTupleCost = Double.parseDouble(costParameters.get("cpu_tuple_cost"));
        String query = """
            SELECT relpages, reltuples
            FROM pg_class
            WHERE relname = ?
        """;
        List<List<String>> result = select(query, indexName);
        if (!result.isEmpty()) {
            double numPages = Double.parseDouble(result.getFirst().getFirst());
            double seqScanCost = calculateSeqScanCost(tableName);
            return (Long) (long) ((seqScanCost - (indexPageCost * numPages)) / cpuTupleCost);
        }

        return 0L;
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

    public static Long getTableSize(String tableName) {
        String query = "SELECT COUNT(*) AS row_count FROM " + tableName;
        return selectOne(query);
    }
}

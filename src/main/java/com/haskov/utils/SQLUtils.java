package com.haskov.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.haskov.bench.V2.select;

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
        List<String> result = select(query, tableName, columnName);
        return !result.isEmpty();
    }

    public static Map<String, String> getCostParameters() {
        String query = """
            SELECT name, setting
            FROM pg_settings
            WHERE name IN ('seq_page_cost', 'random_page_cost', 'cpu_tuple_cost', 'cpu_index_tuple_cost', 'cpu_operator_cost')
        """;

        List<String> result = select(query);
        Map<String, String> costParameters = new HashMap<>();
        for (int i = 0; i < result.size(); i += 2) {
            costParameters.put(result.get(i), result.get(i+1));
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
        List<String> result = select(query, tableName);
        if (!result.isEmpty()) {
            double numPages = Double.parseDouble(result.getFirst());
            double numTuples = Double.parseDouble(result.getLast());
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
        List<String> result = select(query, indexName);
        if (!result.isEmpty()) {
            double numPages = Double.parseDouble(result.getFirst());
            double numTuples = Double.parseDouble(result.getLast());
            return (indexPageCost * numPages) + (cpuTupleCost * numTuples);
        }
        return 0;
    }
}

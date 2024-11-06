package com.haskov.utils;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.haskov.bench.V2.*;

public class SQLUtils {
    /*

    Helpful functions.

     */

    /**
     * @return max element in column of table.
     */
    public static String getMax(String tableName, String columnName) {
        String query = "select max(" + tableName + "." + columnName + ") from " + tableName;
        List<Object> result = selectColumn(query);
        return result.getFirst().toString();
    }

    /**
     * @return min element in column of table.
     */
    public static String getMin(String tableName, String columnName) {
        String query = "select min(" + tableName + "." + columnName + ") from " + tableName;
        List<Object> result = selectColumn(query);
        return result.getFirst().toString();
    }

    public static Long getTableRowCount(String tableName) {
        String query = """
            SELECT reltuples
            FROM pg_class
            WHERE relname = ?
        """;
        return (long) Math.round(selectOne(query, tableName));
    }

    public static Pair<Long, Long> getTablePagesAndRowsCount(String tableName) {
        String query = """
            SELECT relpages, reltuples
            FROM pg_class
            WHERE relname = ?
        """;
        List<List<String>> result = select(query, tableName);
        if (result.isEmpty()) {
            throw new RuntimeException("Table " + tableName + " doesn't exists!");
        }
        return new ImmutablePair<>(
                Math.round(Double.parseDouble(result.getFirst().getFirst())),
                Math.round(Double.parseDouble(result.getFirst().getLast()))
        );
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

    /**
     * @return parameters for query plan cost calculations.
     */
    public static Map<String, String> getCostParameters() {
        String query = """
            SELECT name, setting
            FROM pg_settings
            WHERE name IN ('seq_page_cost', 'random_page_cost', 'cpu_tuple_cost', 'cpu_index_tuple_cost', 'cpu_operator_cost')
        """;

        List<List<String>> result = select(query);
        Map<String, String> costParameters = new HashMap<>();
        for (List<String> strings : result) {
            costParameters.put(strings.getFirst(), strings.get(1));
        }
        return costParameters;
    }

    /**
     * @return correlation in table.
     */
    public static Double getCorrelation(String tableName, String columnName) {
        String query = """
                SELECT correlation
                FROM pg_stats WHERE tablename = ?
                AND attname = ?
                """;
        return (double) (float)selectOne(query, tableName, columnName);
    }

    /**
     * @return visible pages in table.
     */
    public static Integer getVisiblePages(String tableName) {
        String query = """
                SELECT relallvisible
                FROM pg_class WHERE relname = ?
                """;
        return selectOne(query, tableName);
    }

    /**
     * @return work_mem in bytes.
     */
    public static Long getWorkMem() {
        String query = """
                SELECT pg_size_bytes(current_setting('work_mem'))
                """;
        return selectOne(query);
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
}

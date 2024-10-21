package com.haskov.utils;

import java.util.List;

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
}

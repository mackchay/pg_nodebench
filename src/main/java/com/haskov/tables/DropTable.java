package com.haskov.tables;

import com.haskov.bench.V2;

public class DropTable {

    public static void dropTable(String... tableNames) {
        for (String table : tableNames) {
            V2.sql("DROP TABLE IF EXISTS " + table);
        }
    }
}

package com.haskov.tables;

import com.haskov.types.TableData;
import com.haskov.utils.SQLUtils;

import static com.haskov.bench.V2.*;

public class TableBuilder {

    private static String getCreateQuery(TableData data) {
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + data.tableName());
        createTableQuery.append("(");

        if (data.columns() <= 0) {
            throw new IllegalArgumentException("Table " + data.tableName() + " has no columns");
        }
        if (data.isPrimaryKeyReq()) {
            createTableQuery.append(data.tableName()).append("_id").append(" INT NOT NULL PRIMARY KEY");
        }
        else {
            createTableQuery.append("x").append(0).append(" INT NOT NULL");
        }
        for (int i = 1; i < data.columns(); i++) {
            createTableQuery.append(", ").append("x").append(i).append(" INT NOT NULL");
        }
        for (String parentTable: data.parentTables()) {
            createTableQuery.append(", ").append(parentTable).append("_id").
                    append(" INT REFERENCES ").append(parentTable).append("(").
                    append(parentTable).append("_id").append(")").
                    append(" ON DELETE CASCADE");
        }
        createTableQuery.append(")");
        return createTableQuery.toString();
    }

    private static String getInsertQuery(TableData data) {
        StringBuilder insertTableQuery = new StringBuilder("INSERT INTO " + data.tableName());
        insertTableQuery.append("(");
        if (data.isPrimaryKeyReq()) {
            insertTableQuery.append(data.tableName()).append("_id ");
        } else {
            insertTableQuery.append("x").append(0);
        }
        for (int i = 1; i < data.columns(); i++) {
            insertTableQuery.append(", x").append(i);
        }
        for (String parentTable: data.parentTables()) {
            insertTableQuery.append(", ").append(parentTable).append("_id");
        }

        insertTableQuery.append(") VALUES ");
        for (int j = 0; j < data.size(); j++) {
            insertTableQuery.append("(");
            for (int i = 0; i < data.columns(); i++) {
                insertTableQuery.append(j);
                if (i != data.columns() - 1) {
                    insertTableQuery.append(", ");
                }
            }
            for (int i = 0; i < data.parentTables().size(); i++) {
                insertTableQuery.append(", ").append(j % SQLUtils.getTableRowCount(data.parentTables().get(i)));
            }
            insertTableQuery.append(")");
            if (j != data.size() - 1) {
                insertTableQuery.append(", ");
            }
        }

        return insertTableQuery.toString();
    }

    public static String getIndexQuery(TableData data) {
        if (data.isIndexRequiredList().isEmpty()) {
            return "";
        }
        if (data.isIndexRequiredList().size() != data.columns()) {
            throw new IllegalArgumentException("Columns don't match");
        }

        StringBuilder indexQuery = new StringBuilder("CREATE INDEX " + data.tableName() + "_idx on " +
                data.tableName() + " (");
        if (!data.isPrimaryKeyReq()) {
            indexQuery.append("x").append(0).append(",");
        }
        for (int i = 1; i < data.columns(); i++) {
            if (data.isIndexRequiredList().get(i)) {
                indexQuery.append("x").append(i).append(",");
            }
        }
        indexQuery.deleteCharAt(indexQuery.length() - 1);
        for (String parentTable: data.parentTables()) {
            indexQuery.append(", ").append(parentTable).append("_id");
        }
        indexQuery.append(")");
        return indexQuery.toString();
    }

    public static void createRandomTable(TableData data) {
        sql(getCreateQuery(data));
        sql(getInsertQuery(data));
        sql(getIndexQuery(data));
    }

}

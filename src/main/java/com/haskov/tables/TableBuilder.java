package com.haskov.tables;

import com.haskov.utils.SQLUtils;

import static com.haskov.bench.V2.*;

public class TableBuilder {

    private static String getCreateQuery(TableData data) {
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + data.getTableName());
        createTableQuery.append("(");

        if (data.getColumns() <= 0) {
            throw new IllegalArgumentException("Table " + data.getTableName() + " has no columns");
        }
        if (data.isPrimaryKeyReq()) {
            createTableQuery.append(data.getTableName()).append("_id").append(" INT NOT NULL PRIMARY KEY");
        }
        else {
            createTableQuery.append("x").append(0).append(" INT NOT NULL");
        }
        for (int i = 1; i < data.getColumns(); i++) {
            createTableQuery.append(", ").append("x").append(i).append(" INT NOT NULL");
        }
        for (String parentTable: data.getParentTables()) {
            createTableQuery.append(", ").append(parentTable).append("_id").
                    append(" REFERENCES ").append(parentTable).append("(").
                    append(parentTable).append("_id").append(")").
                    append(" ON DELETE CASCADE");
        }
        createTableQuery.append(")");
        return createTableQuery.toString();
    }

    private static String getInsertQuery(TableData data) {
        StringBuilder insertTableQuery = new StringBuilder("INSERT INTO " + data.getTableName());
        insertTableQuery.append("(");
        if (data.isPrimaryKeyReq()) {
            insertTableQuery.append(data.getTableName()).append("_id ");
        } else {
            insertTableQuery.append("x").append(0).append(" INT NOT NULL ");
        }
        for (int i = 1; i < data.getColumns(); i++) {
            insertTableQuery.append(", x").append(i);
        }
        for (String parentTable: data.getParentTables()) {
            insertTableQuery.append(", ").append(parentTable).append("_id");
        }

        insertTableQuery.append(") VALUES ");
        for (int j = 0; j < data.getSize(); j++) {
            insertTableQuery.append("(");
            for (int i = 0; i < data.getColumns(); i++) {
                insertTableQuery.append(j);
                if (i != data.getColumns() - 1) {
                    insertTableQuery.append(", ");
                }
            }
            for (int i = 0; i < data.getParentTables().size(); i++) {
                insertTableQuery.append(", ").append(i % SQLUtils.getTableRowCount(data.getParentTables().get(i)));
            }
            insertTableQuery.append(")");
            if (j != data.getSize() - 1) {
                insertTableQuery.append(", ");
            }
        }

        return insertTableQuery.toString();
    }

    //TODO fix index query bugs with primary key.
    public static String getIndexQuery(TableData data) {
        if (data.getIsIndexRequiredList().isEmpty()) {
            return "";
        }
        if (data.getIsIndexRequiredList().size() != data.getColumns()) {
            throw new IllegalArgumentException("Columns don't match");
        }

        StringBuilder indexQuery = new StringBuilder("CREATE INDEX " + data.getTableName() + "_idx on (");
        for (int i = 0; i < data.getColumns(); i++) {
            if (data.getIsIndexRequiredList().get(i)) {
                indexQuery.append("x").append(0);
                if (i != data.getParentTables().size() - 1) {
                    indexQuery.append(", ");
                }
            }
        }
        for (String parentTable: data.getParentTables()) {
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

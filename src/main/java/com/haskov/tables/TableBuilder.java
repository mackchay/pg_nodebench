package com.haskov.tables;

import com.haskov.bench.V2;
import com.haskov.types.InsertType;
import com.haskov.types.TableBuildResult;
import com.haskov.types.TableData;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.haskov.bench.V2.*;
import static com.haskov.utils.MathUtils.getRandomBooleanList;

public class TableBuilder {
    private static Map<String, Integer> tableNames = new HashMap<>();

    public static TableBuildResult buildRandomTable(String tableName, long tableSize, InsertType insertType) {
        if (SQLUtils.isTableExists(tableName) &&
                SQLUtils.getTablePagesAndRowsCount(tableName).getRight().equals(tableSize)) {
            if (tableNames.containsKey(tableName)) {
                tableName += tableNames.get(tableName);
            }
        }

        DropTable.dropTable(tableName);
        int maxColumns = 10;
        Random random = new Random();
        int columnCount = random.nextInt(3, maxColumns);
        List<Boolean> randomBooleanList = getRandomBooleanList(columnCount);
        if (!randomBooleanList.subList(1, randomBooleanList.size()).contains(true)) {
            randomBooleanList.set(1, true);
        }
        if (!randomBooleanList.subList(1, randomBooleanList.size()).contains(false)) {
            randomBooleanList.set(2, false);
        }
        List<String> tableQueries = TableBuilder.createRandomTable(new TableData(
                tableName,
                new ArrayList<>(),
                columnCount,
                tableSize,
                randomBooleanList,
                new ArrayList<>(),
                true,
                insertType
        ));

        V2.sql("vacuum freeze analyze " + tableName);
        tableNames.put(tableName, 1);
        return new TableBuildResult(tableName, tableQueries);
    }


    /**
     * Insert type is ASCENDING by default.
     * @param tableName name of table
     * @param tableSize size of table
     */
    public static TableBuildResult buildRandomTable(String tableName, long tableSize) {
        return buildRandomTable(tableName, tableSize, InsertType.ASCENDING);
    }

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

    private static String checkInsertType(InsertType insertType, Integer x, long max) {
        StringBuilder query = new StringBuilder();
        Random random = new Random();
        switch (insertType) {
            case ASCENDING: query.append((int)x); break;
            case DESCENDING: query.append(max - x); break;
            default: query.append(random.nextLong(max)); break;
        }
        return query.toString();
    }


    private static String insertData(TableData data) {
        StringBuilder query = new StringBuilder();
        List<Integer> valuesList = new ArrayList<>();
        if (data.insertType().equals(InsertType.RANDOM)) {
            for (int i = 0; i < data.size(); i++) {
                valuesList.add(i);
            }
            Collections.shuffle(valuesList);
        }


        for (int j = 0; j < data.size(); j++) {
            query.append("(");
            for (int i = 0; i < data.columns(); i++) {
                if (data.insertType().equals(InsertType.RANDOM)) {
                    query.append(valuesList.get(j));
                } else {
                    query.append(checkInsertType(data.insertType(), j, data.size()));
                }
                if (i != data.columns() - 1) {
                    query.append(", ");
                }
            }
            for (int i = 0; i < data.parentTables().size(); i++) {
                query.append(", ");
                if (data.insertType().equals(InsertType.ASCENDING)) {
                    query.append(j % SQLUtils.getTableRowCount(data.parentTables().get(i)));
                } else {
                    query.append(data.size() - (j % SQLUtils.getTableRowCount(data.parentTables().get(i))));
                }
            }
            query.append(")");
            if (j != data.size() - 1) {
                query.append(", ");
            }
        }
        return query.toString();
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
        insertTableQuery.append(insertData(data));

        return insertTableQuery.toString();
    }

    public static String getIndexQuery(TableData data) {
        List<Boolean> isIndexRequiredList = data.isIndexRequiredList();
        if (isIndexRequiredList.isEmpty()
                || !isIndexRequiredList.contains(true)
                || (data.isPrimaryKeyReq() &&
                !isIndexRequiredList.subList(1, isIndexRequiredList.size()).contains(true))) {
            return "";
        }
        if (data.isIndexRequiredList().size() != data.columns()) {
            throw new IllegalArgumentException("Columns don't match");
        }

        StringBuilder indexQuery = new StringBuilder("CREATE INDEX " + data.tableName() + "_idx on " +
                data.tableName() + " (");
        if (!data.isPrimaryKeyReq() && data.isIndexRequiredList().getFirst()) {
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

    private static List<String> getIndexQueries(TableData data) {
        List<Boolean> isIndexRequiredList = data.isIndexRequiredList();
        if (isIndexRequiredList.isEmpty()
                || !isIndexRequiredList.contains(true)
                || (data.isPrimaryKeyReq() &&
                !isIndexRequiredList.subList(1, isIndexRequiredList.size()).contains(true))) {
            return new ArrayList<>();
        }
        if (data.isIndexRequiredList().size() != data.columns()) {
            throw new IllegalArgumentException("Columns don't match");
        }

        List<String> queries = new ArrayList<>();
        if (!data.isPrimaryKeyReq() && data.isIndexRequiredList().getFirst()) {
            queries.add("CREATE INDEX " + data.tableName() + "_idx0 ON " + data.tableName() + " (x0)");
        }

        for (int i = 1; i < data.columns(); i++) {
            if (data.isIndexRequiredList().get(i)) {
                queries.add("CREATE INDEX " + data.tableName() + "_idx" +
                        i + " ON " + data.tableName() + " (x" + i + ")");
            }
        }
        return queries;
    }

    public static List<String> createRandomTable(TableData data) {
        List<String> tableQueries = new ArrayList<>();
        String createTableQuery = getCreateQuery(data);
        tableQueries.add(createTableQuery);
        String insertQuery = getInsertQuery(data);
        tableQueries.add(insertQuery);
        sql(getCreateQuery(data));
        sql(getInsertQuery(data));
        for (String query : getIndexQueries(data)) {
            tableQueries.add(query);
            sql(query);
        }
        return tableQueries;
    }

    public static List<String> addForeignKey(String childTableName, String parentTableName,
                                             String joinNode) {
        List<String> sqlQueries = new ArrayList<>();
//        String newColumnName = parentTableName + "_id";
//        String alterQuery = "ALTER TABLE " + childTableName +
//                " ADD COLUMN " + newColumnName + " INT ";
//        String updateQuery, foreignKeyQuery;
//        updateQuery = "UPDATE " + childTableName + " " +
//                "SET " + newColumnName + " = "
//                + parentTableName + "." + newColumnName + " " +
//                "FROM " + parentTableName + " " +
//                "WHERE " + childTableName + ".x1 = " +
//                parentTableName + ".x1";
//        foreignKeyQuery = "ALTER TABLE " + childTableName + " " +
//                "ADD CONSTRAINT " + newColumnName + " " +
//                "FOREIGN KEY (" + newColumnName + ") " +
//                "REFERENCES " + parentTableName + "(" + newColumnName + ") " +
//                "ON DELETE CASCADE";
//        sqlQueries.add(alterQuery);
//        sqlQueries.add(updateQuery);
//        sqlQueries.add(foreignKeyQuery);
//        sql(alterQuery);
//        sql(updateQuery);
//        sql(foreignKeyQuery);
        return sqlQueries;
    }
}

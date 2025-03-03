package com.haskov.tables;

import com.haskov.bench.V2;
import com.haskov.types.InsertType;
import com.haskov.types.TableBuildResult;
import com.haskov.types.TableData;
import com.haskov.types.TableIndexType;
import com.haskov.utils.SQLUtils;

import java.util.*;

import static com.haskov.bench.V2.*;
import static com.haskov.utils.MathUtils.getRandomBooleanList;

public class TableBuilder {
    private static Map<String, Integer> tableNames = new HashMap<>();

    public static TableBuildResult buildRandomTable(String tableName, long tableSize,
                                                    InsertType insertType, TableIndexType tableIndexType) {
        if (SQLUtils.isTableExists(tableName) &&
                SQLUtils.getTableRowCount(tableName).equals(tableSize)) {
            if (tableNames.containsKey(tableName)) {
                int index = tableNames.get(tableName);
                tableNames.put(tableName, index + 1);
                tableName += index;
            }
        }

        DropTable.dropTable(tableName);
        int maxColumns = 10;
        Random random = new Random();
        int columnCount = random.nextInt(3, maxColumns);
        List<Boolean> randomBooleanList = getIndexList(tableIndexType, columnCount);
        boolean isPrimaryKey = true;
        if (tableIndexType.equals(TableIndexType.FULL_NON_INDEX)
                || tableIndexType.equals(TableIndexType.FULL_SAME_INDEX)) {
            isPrimaryKey = false;
        }

        TableData data = new TableData(
                tableName,
                new ArrayList<>(),
                columnCount,
                tableSize,
                randomBooleanList,
                new ArrayList<>(),
                isPrimaryKey,
                insertType,
                tableIndexType
        );
        List<String> tableQueries = TableBuilder.createRandomTable(data);

        if (tableSize > 10000) {
            setStatistics(data);
        }

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
        return buildRandomTable(tableName, tableSize, InsertType.ASCENDING, TableIndexType.RANDOM);
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

    public static void setStatistics(TableData data) {
        List<String> columnsList = new ArrayList<>();
        String tableIdColumns = data.tableName() + "_id";
        if (!data.isPrimaryKeyReq()) {
            tableIdColumns = "x0";
        }
        columnsList.add(tableIdColumns);
        for (int i = 1; i < data.columns(); i++) {
           columnsList.add("x" + i);
        }
        SQLUtils.setStatistics(data.tableName(), columnsList, (int)data.size() / 10);
    }

    public static List<String> createRandomTable(TableData data) {
        List<String> tableQueries = new ArrayList<>();
        String createTableQuery = getCreateQuery(data);
        tableQueries.add(createTableQuery);
        String insertQuery = getInsertQuery(data);
        tableQueries.add(insertQuery);
        sql(getCreateQuery(data));
        sql(getInsertQuery(data));
        if (data.indexType().equals(TableIndexType.FULL_SAME_INDEX)) {
            String indexQuery = getIndexQuery(data);
            tableQueries.add(indexQuery);
            sql(indexQuery);
            return tableQueries;
        }
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

    private static List<Boolean> getIndexList(TableIndexType indexType, int size) {
        List<Boolean> booleansList = getRandomBooleanList(size);
        switch (indexType) {
            case RANDOM:
                if (!booleansList.subList(1, booleansList.size()).contains(true)) {
                    booleansList.set(1, true);
                }
                if (!booleansList.subList(1, booleansList.size()).contains(false)) {
                    booleansList.set(2, false);
                }
                return booleansList;
            case FULL_SAME_INDEX:
            case FULL_UNIQUE_INDEX:
                booleansList.forEach(e -> booleansList.set(booleansList.indexOf(e), true));
                return booleansList;
            case FULL_NON_INDEX:
                booleansList.forEach(e -> booleansList.set(booleansList.indexOf(e), false));
                return booleansList;
            default:
                throw new RuntimeException("Unsupported index type: " + indexType);
        }
    }
}

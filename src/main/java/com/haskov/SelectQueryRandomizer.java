package com.haskov;

import com.haskov.bench.V2;

import java.sql.*;
import java.util.*;

public class SelectQueryRandomizer {

    private final Random random = new Random();

    // Метод для генерации случайного SELECT запроса с возможностью JOIN и UNION
    // TODO: create node condition to choose UNION ALL or JOIN.
    public String generateRandomSelect(String[] tableNames) throws SQLException {
        List<String> selectedColumns = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT ");

        for (String tableName : tableNames) {
            Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(tableName);
            selectedColumns.addAll(generateRandomColumns(tableName, columnsAndTypes));
        }

        // Генерируем SELECT часть запроса
        sql.append(String.join(", ", selectedColumns)).append(" FROM ").append(tableNames[0]);

        // Генерируем случайный JOIN с другими таблицами
        if (tableNames.length > 1) {
            for (int i = 1; i < tableNames.length; i++) {
                String joinType = getRandomJoinType();
                sql.append(" ").append(joinType).append(" ").append(tableNames[i])
                        .append(" ON ").append(generateRandomJoinCondition(tableNames[0], tableNames[i]));
            }
        }

        // Генерация WHERE условий (случайные фильтры)
        if (random.nextBoolean()) {
            sql.append(" WHERE ").append(generateRandomWhereCondition(tableNames));
        }

        // Можно добавить UNION или другие сложные операторы по желанию
        if (random.nextBoolean() && tableNames.length > 1) {
            sql.append(" UNION ALL ").append(generateRandomSelect(new String[] {tableNames[random.nextInt(tableNames.length)]}));
        }

        return sql.toString() + ";";
    }

    // Генерация случайных столбцов для SELECT
    private List<String> generateRandomColumns(String tableName, Map<String, String> columnsAndTypes) {
        List<String> selectedColumns = new ArrayList<>();
        List<String> columnNames = new ArrayList<>(columnsAndTypes.keySet());

        int columnCount = random.nextInt(columnNames.size()) + 1;
        Collections.shuffle(columnNames);

        for (int i = 0; i < columnCount; i++) {
            selectedColumns.add(tableName + "." + columnNames.get(i));
        }

        return selectedColumns;
    }

    // Генерация случайного типа JOIN
    private String getRandomJoinType() {
        String[] joinTypes = {"INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"};
        return joinTypes[random.nextInt(joinTypes.length)];
    }

    // TODO: make condition random
    private String generateRandomJoinCondition(String table1, String table2) throws SQLException {
        return table1 + ".id = " + table2 + ".id";
    }

    // Генерация случайного WHERE условия
    private String generateRandomWhereCondition(String[] tableNames) throws SQLException {
        StringBuilder whereClause = new StringBuilder();

        // Выбираем случайную таблицу и случайный столбец для условия WHERE
        String randomTable = tableNames[random.nextInt(tableNames.length)];
        Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(randomTable);
        List<String> columnNames = new ArrayList<>(columnsAndTypes.keySet());
        String randomColumn = columnNames.get(random.nextInt(columnNames.size()));
        String columnType = columnsAndTypes.get(randomColumn);

        whereClause.append(randomTable).append(".").append(randomColumn).append(" ");

        switch (columnType.toLowerCase()) {
            case "int4":
            case "integer":
            case "int":
                whereClause.append("= ").append(random.nextInt(100));
                break;

            case "varchar":
            case "text":
                whereClause.append("LIKE '%").append(getRandomString(random.nextInt(1, 50))).append("%'");
                break;

            case "bool":
            case "boolean":
                whereClause.append("= ").append(random.nextBoolean());
                break;

            default:
                whereClause.append("IS NOT NULL");
        }

        return whereClause.toString();
    }

    // Генератор случайной строки
    private String getRandomString(int length) {
        String characters = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            result.append(characters.charAt(random.nextInt(characters.length())));
        }
        return result.toString();
    }
}


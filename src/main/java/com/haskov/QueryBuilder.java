package com.haskov;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class QueryBuilder {

    private String tableName;
    private List<String> selectColumns = new ArrayList<>();
    private List<String> whereConditions = new ArrayList<>();
    private List<String> orderByColumns = new ArrayList<>();
    private Integer limitValue;
    private final Random random = new Random();

    // Метод для указания таблицы
    public QueryBuilder from(String table) {
        this.tableName = table;
        return this;
    }

    // Метод для указания столбцов в SELECT
    public QueryBuilder select(String... columns) {
        this.selectColumns.addAll(Arrays.asList(columns));
        return this;
    }

    // Метод для указания столбцов в SELECT
    public QueryBuilder select(List<String> columns) {
        this.selectColumns.addAll(columns);
        return this;
    }

    // Метод для добавления условий WHERE
    public QueryBuilder where(String condition) {
        this.whereConditions.add(condition);
        return this;
    }

    //TODO make an operators =, <, >
    public QueryBuilder randomWhere(String table, String column, String type) {
        StringBuilder whereClause = new StringBuilder();
        whereClause.append(table).append(".").append(column).append(" ");
        this.whereConditions.add(randomWhereTypes(whereClause, type));
        return this;
    }

    private String randomWhereTypes(StringBuilder whereClause, String type) {
        switch (type.toLowerCase()) {
            case "int4":
            case "integer":
            case "int":
                whereClause.append("= ").append(random.nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE));
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

    // Метод для указания сортировки ORDER BY
    public QueryBuilder orderBy(String... columns) {
        this.orderByColumns.addAll(Arrays.asList(columns));
        return this;
    }

    // Метод для указания лимита LIMIT
    public QueryBuilder limit(int limit) {
        this.limitValue = limit;
        return this;
    }

    // Метод для сборки финального запроса
    public String build() {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalStateException("Table name must be specified");
        }

        StringBuilder query = new StringBuilder();

        // SELECT part
        if (selectColumns.isEmpty()) {
            query.append("SELECT *");
        } else {
            query.append("SELECT ").append(String.join(", ", selectColumns));
        }

        // FROM part
        query.append(" FROM ").append(tableName);

        // WHERE part
        if (!whereConditions.isEmpty()) {
            query.append(" WHERE ").append(String.join(" AND ", whereConditions));
        }

        // ORDER BY part
        if (!orderByColumns.isEmpty()) {
            query.append(" ORDER BY ").append(String.join(", ", orderByColumns));
        }

        // LIMIT part
        if (limitValue != null) {
            query.append(" LIMIT ").append(limitValue);
        }

        return query.toString();
    }

    public static void main(String[] args) {
        QueryBuilder query = new QueryBuilder()
                .select("id", "name", "age")
                .from("users")
                .where("age > 18")
                .where("name LIKE 'John%'").where("age < 30")
                .orderBy("age", "name")
                .limit(10);

        System.out.println(query.build());
        // Вывод: SELECT id, name, age FROM users WHERE age > 18 AND name LIKE 'John%' ORDER BY age, name LIMIT 10
    }
}

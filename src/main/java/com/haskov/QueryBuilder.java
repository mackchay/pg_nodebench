package com.haskov;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryBuilder {

    private String tableName;
    private List<String> selectColumns = new ArrayList<>();
    private List<String> whereConditions = new ArrayList<>();
    private List<String> orderByColumns = new ArrayList<>();
    private Integer limitValue;

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

    // Метод для добавления условий WHERE
    public QueryBuilder where(String condition) {
        this.whereConditions.add(condition);
        return this;
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
                .where("name LIKE 'John%'")
                .orderBy("age", "name")
                .limit(10);

        System.out.println(query.build());
        // Вывод: SELECT id, name, age FROM users WHERE age > 18 AND name LIKE 'John%' ORDER BY age, name LIMIT 10
    }
}

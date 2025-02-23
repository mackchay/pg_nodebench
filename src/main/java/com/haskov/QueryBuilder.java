package com.haskov;

import com.haskov.types.JoinData;
import com.haskov.types.JoinType;
import com.haskov.types.ReplaceOrAdd;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class QueryBuilder {

    private String tableName;
    @Getter
    private final List<String> selectColumns = new ArrayList<>();
    private final List<String> whereConditions = new ArrayList<>();
    private final List<String> orderByColumns = new ArrayList<>();
    private final List<JoinData> joins = new ArrayList<>();
    private Integer limitValue;
    private final Random random = new Random();
    private final List<String> groupByColumns = new ArrayList<>();

    //Список запросов входящих в Union ALL
    private final List<QueryBuilder> unionQueryBuilders = new ArrayList<>();
    //Список запросов входящих в Intersect
    private final List<QueryBuilder> intersectQueryBuilders = new ArrayList<>();

    //Минимальное число строк которое можно прочитать
    private Long minTuples = 0L;
    //Максимальное число строк которое можно прочитать
    private Long maxTuples = Long.MAX_VALUE;

    private Long min = 0L;
    private Long max = Long.MAX_VALUE;

    //Максимальное количество столбцов среди всех запросов с UNION ALL
    private int maxSelectColumns = 0;

    //Нужно ли получить данные без дубликатов
    @Setter
    private boolean isDistinct = false;

    // Метод для указания таблицы
    public QueryBuilder from(String table) {
        tableName = table;
        return this;
    }

    public void setMinMaxTuples(long minTuples, long maxTuples) {
        if (this.minTuples < minTuples) {
            this.minTuples = minTuples;
        }
        if (this.maxTuples > maxTuples) {
            this.maxTuples = maxTuples;
        }
    }

    public void setMinMaxTuplesForce(long minTuples, long maxTuples) {
        this.minTuples = minTuples;
        this.maxTuples = maxTuples;
    }


    public List<String> getAllSelectColumns() {
        List<String> allSelectColumns = new ArrayList<>(selectColumns);
        for (QueryBuilder queryBuilder : unionQueryBuilders) {
            allSelectColumns.addAll(queryBuilder.getAllSelectColumns());
        }
        return allSelectColumns;
    }

    /**
     * @return minTuples, maxTuples
     */
    public Pair<Long, Long> getMinMaxTuples() {
        return new ImmutablePair<>(minTuples, maxTuples);
    }

    public void setMinMax(long min, long max) {
        this.min = min;
        this.max = max;
    }

    // Метод для указания столбцов в SELECT
    public QueryBuilder select(String... columns) {
        this.selectColumns.addAll(Arrays.asList(columns));
        syncMaxSelectColumns();
        return this;
    }

    // Метод для указания столбцов в SELECT
    public QueryBuilder select(List<String> columns) {
        this.selectColumns.addAll(columns);
        return this;
    }

    // Метод для добавления запроса с UNION ALL
    public QueryBuilder unionAll(QueryBuilder queryBuilder) {
        // Проверяем количество столбцов в обоих запросах и выравниваем их
        int currentColumnsSize = this.selectColumns.size();
        int queryColumnsSize = queryBuilder.selectColumns.size();

        if (currentColumnsSize < queryColumnsSize) {
            // Добавляем пустые столбцы (NULL) в текущий запрос
            for (int i = currentColumnsSize; i < queryColumnsSize; i++) {
                this.selectColumns.add("NULL::INT");
            }
        } else if (currentColumnsSize > queryColumnsSize) {
            // Добавляем пустые столбцы (NULL) в запрос, с которым выполняем объединение
            for (int i = queryColumnsSize; i < currentColumnsSize; i++) {
                queryBuilder.selectColumns.add("NULL::INT");
            }
        }

        unionQueryBuilders.add(queryBuilder);
        updateMaxSelectColumns();
        return this;
    }

    // Метод для синхронизации всех частей запроса с максимальным количеством столбцов
    private void updateMaxSelectColumns() {
        maxSelectColumns = Math.max(maxSelectColumns, this.selectColumns.size());
    }

    public boolean IsSelectColumnsEmpty() {
        return selectColumns.isEmpty();
    }
    // Метод для добавления условий WHERE

    public QueryBuilder where(String condition) {
        this.whereConditions.add(condition);
        return this;
    }

    // Метод для добавления случайного метода
    public QueryBuilder randomWhere(String table, String column) {
        this.select(table + "." + column);

        long tuples = random.nextLong(minTuples, maxTuples + 1);
        //long tuples = maxTuples;
        long radius = random.nextLong(min, max - tuples + 2);

        this.where(table + "." + column + ">=" + radius).
                where(table + "." + column + "<" + (radius + tuples));

        return this;
    }

    public QueryBuilder join(JoinData join) {
        joins.add(join);
        return this;
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

    // Метод для указания группировки GROUP BY
    public QueryBuilder groupBy(String... columns) {
        this.groupByColumns.addAll(Arrays.asList(columns));
        return this;
    }

    // Метод для указания запросов с общими столбцами
    public QueryBuilder intersect(QueryBuilder queryBuilder) {
        // Проверяем количество столбцов в обоих запросах и выравниваем их
        int currentColumnsSize = this.selectColumns.size();
        int queryColumnsSize = queryBuilder.selectColumns.size();

        if (currentColumnsSize < queryColumnsSize) {
            // Добавляем пустые столбцы (NULL) в текущий запрос
            for (int i = currentColumnsSize; i < queryColumnsSize; i++) {
                this.selectColumns.add("NULL::INT");
            }
        } else if (currentColumnsSize > queryColumnsSize) {
            // Добавляем пустые столбцы (NULL) в запрос, с которым выполняем объединение
            for (int i = queryColumnsSize; i < currentColumnsSize; i++) {
                queryBuilder.selectColumns.add("NULL::INT");
            }
        }

        intersectQueryBuilders.add(queryBuilder);
        updateMaxSelectColumns();
        return this;
    }

    public QueryBuilder replaceUnionAllWithIntersect() {
        intersectQueryBuilders.addAll(unionQueryBuilders);
        unionQueryBuilders.clear();
        for (QueryBuilder queryBuilder : intersectQueryBuilders) {
            queryBuilder.replaceUnionAllWithIntersect();
        }
        return this;
    }

    //Методы для указания столбцов, над которыми будет агрегатные операции Aggregate
    public void count(String column, ReplaceOrAdd replaceOrAdd) {
        if (replaceOrAdd == ReplaceOrAdd.REPLACE) {
            if (selectColumns.contains(column)) {
                selectColumns.remove(column);
                selectColumns.add("COUNT(" + column + ")");
            }
        } else {
            if (selectColumns.contains(column)) {
                selectColumns.add("COUNT(" + column + ")");
                groupByColumns.add(column);
            }
        }
    }

    public QueryBuilder max(String table, String column, ReplaceOrAdd replaceOrAdd) {
        if (replaceOrAdd == ReplaceOrAdd.REPLACE) {
            if (selectColumns.removeIf(e -> e.equals(table + "." + column))) {
                selectColumns.add("MAX(" + table + "." + column + ")");
            }
        } else {
            selectColumns.add("MAX(" + table + "." + column + ")");
        }
        return this;
    }

    public QueryBuilder min(String table, String column, ReplaceOrAdd replaceOrAdd) {
        if (replaceOrAdd == ReplaceOrAdd.REPLACE) {
            if (selectColumns.removeIf(e -> e.equals(table + "." + column))) {
                selectColumns.add("MIN(" + table + "." + column + ")");
            }
        } else {
            selectColumns.add("MIN(" + table + "." + column + ")");
        }
        return this;
    }

    public void syncMaxSelectColumns() {
        while (selectColumns.size() > maxSelectColumns && selectColumns.contains("NULL::INT") && maxSelectColumns != 0) {
            selectColumns.remove("NULL::INT");
        }
        if (maxSelectColumns != 0) {
            List<String> subList = new ArrayList<>(selectColumns.subList(0, maxSelectColumns));
            selectColumns.clear();
            selectColumns.addAll(subList);
        }
    }

    // Метод для сборки финального запроса
    public String build() {
        if (tableName.isEmpty()) {
            throw new IllegalStateException("Table name must be specified");
        }

        if (selectColumns.isEmpty()) {
            throw new RuntimeException("Column names must be specified");
        }

        StringBuilder query = new StringBuilder();

        // SELECT
        if (isDistinct) {
            query.append("SELECT DISTINCT ").append(String.join(", ", selectColumns));
        } else {
            query.append("SELECT ").append(String.join(", ", selectColumns));
        }


        // FROM part
        query.append(" FROM ").append(String.join(",", tableName));

        for (JoinData join : joins) {
            if (join.joinType().equals(JoinType.CROSS)) {
                query.append(" ").append(join.joinType()).append(" JOIN ").append(join.childTable()).append(" ");
                continue;
            }
            if (join.joinType().equals(JoinType.NON_EQUAL)) {
                query.append(" ").append(" JOIN ").append(join.childTable()).append(" ON ").
                        append(join.childTable()).append(".").append(join.foreignKeyColumn())
                        .append(" != ").append(join.parentTable()).append(".").append(join.referencedColumn());
                continue;
            }
            if (join.joinType().equals(JoinType.USUAL)) {
                query.append(" ").append(" JOIN ").append(join.childTable()).append(" ON ").
                        append(join.parentTable()).append(".").append(join.referencedColumn())
                        .append(" = ").append(join.childTable()).append(".")
                        .append(join.foreignKeyColumn());
                continue;
            }
            query.append(" ").append(join.joinType()).append(" JOIN ").append(join.childTable()).append(" ON ").
                    append(join.parentTable()).append(".").append(join.referencedColumn())
                    .append(" = ").append(join.childTable()).append(".")
                    .append(join.foreignKeyColumn());
        }

        // WHERE part
        if (!whereConditions.isEmpty()) {
            query.append(" WHERE (").append(String.join(") AND (", whereConditions)).append(")");
        }


        if (!groupByColumns.isEmpty()) {
            query.append(" GROUP BY ").append(String.join(", ", groupByColumns));
        }

        // INTERSECT part
        for (QueryBuilder intersectQueryBuilder : intersectQueryBuilders) {
            query.append(" INTERSECT ").append(intersectQueryBuilder.build());
        }

        //UNION ALL part
        for (QueryBuilder unionQueryBuilder : unionQueryBuilders) {
            query.append(" UNION ALL ").append(unionQueryBuilder.build());
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
                .where("age < 30")
                .orderBy("age", "name")
                .limit(10);

        System.out.println(query.build());
        // Вывод: SELECT id, name, age FROM users WHERE age > 18 AND name LIKE 'John%' ORDER BY age, name LIMIT 10
    }
}

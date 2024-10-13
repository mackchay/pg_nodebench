package com.haskov;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.haskov.bench.V2;


public class InsertQueryRandomizer {

    private final Random random = new Random();

    public String generateRandomInsert(String tableName) throws SQLException {
        Map<String, String> columnsAndTypes = V2.getColumnsAndTypes(tableName);
        StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " (");
        StringBuilder values = new StringBuilder("VALUES (");

        for (Map.Entry<String, String> entry : columnsAndTypes.entrySet()) {
            String columnName = entry.getKey();
            String columnType = entry.getValue();

            sql.append(columnName).append(", ");
            values.append(generateRandomValue(columnType)).append(", ");
        }

        // Убираем последние запятые
        sql.setLength(sql.length() - 2);
        values.setLength(values.length() - 2);

        sql.append(") ").append(values).append(");");
        return sql.toString();
    }

    private String generateRandomValue(String columnType) {
        switch (columnType.toLowerCase()) {
            case "int4":
            case "integer":
            case "int":
                return String.valueOf(random.nextInt(1000)); // случайное целое число

            case "varchar":
            case "text":
                return "'" + getRandomString(10) + "'"; // случайная строка

            case "bool":
            case "boolean":
                return String.valueOf(random.nextBoolean()); // случайное булевое значение

            case "float4":
            case "float8":
            case "real":
            case "double precision":
                return String.valueOf(random.nextDouble() * 100); // случайное число с плавающей точкой

            case "date":
                return "'2023-01-01'"; // фиксированная дата

            default:
                return "NULL";
        }
    }

    private String getRandomString(int length) {
        String characters = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            result.append(characters.charAt(random.nextInt(characters.length())));
        }
        return result.toString();
    }
}


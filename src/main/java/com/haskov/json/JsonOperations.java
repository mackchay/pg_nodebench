package com.haskov.json;

import com.google.gson.*;
import org.postgresql.util.PGobject;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import static com.haskov.bench.V2.selectColumn;

public class JsonOperations {

    public static JsonObject explainResultsJson(String sql, Object... binds) {
        List<PGobject> pGobjectList = selectColumn("explain (analyze, verbose, buffers, format json) " + sql, binds);
        Gson gson = new Gson();
        JsonArray jsonArray = gson.fromJson(pGobjectList.getFirst().getValue(), JsonArray.class);
        return jsonArray.get(0).getAsJsonObject();
    }

    public static List<PgJsonPlan> getAllPlans(JsonObject jsonObject) {
        return getAllPlansRecursive(jsonObject, new ArrayList<>());
    }

    private static List<PgJsonPlan> getAllPlansRecursive(JsonObject jsonObject, List<PgJsonPlan> list) {
        list.add(new PgJsonPlan(jsonObject));
        if (jsonObject.getAsJsonArray("Plans") != null) {
            JsonArray jsonArray = jsonObject.getAsJsonArray("Plans");
            for (JsonElement jsonElement : jsonArray) {
                return getAllPlansRecursive(jsonElement.getAsJsonObject(), list);
            }
        }
        return list;
    }

    private static PgJsonPlan findNodeRecursive(JsonObject jsonObject, String nodeType, String parameterKey,
                                                String parameterData) {
        if (nodeType.equals(jsonObject.get("Node Type").getAsString()) && jsonObject.has(parameterKey)
                && parameterData.equals(jsonObject.get(parameterKey).getAsString())) {
            return new PgJsonPlan(jsonObject);
        }

        if (jsonObject.has("Plans")) {
            JsonArray jsonArray = jsonObject.getAsJsonArray("Plans");

            for (JsonElement jsonElement : jsonArray) {
                PgJsonPlan result = findNodeRecursive(jsonElement.getAsJsonObject(), nodeType, parameterKey, parameterData);

                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    private static PgJsonPlan findNodeRecursive(JsonObject jsonObject, String nodeType) {
        if (nodeType.equals(jsonObject.get("Node Type").getAsString())) {
            return new PgJsonPlan(jsonObject);
        }

        if (jsonObject.has("Plans")) {
            JsonArray jsonArray = jsonObject.getAsJsonArray("Plans");
            for (JsonElement jsonElement : jsonArray) {
                PgJsonPlan result = findNodeRecursive(jsonElement.getAsJsonObject(), nodeType);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    public static PgJsonPlan findNode(JsonObject jsonObject, String nodeType, String parameterKey, String parameterData) {
        if (!jsonObject.has("Plan")) {
            return null;
        }
        return findNodeRecursive(jsonObject.getAsJsonObject("Plan"), nodeType, parameterKey, parameterData);
    }

    public static PgJsonPlan findNode(JsonObject jsonObject, String nodeType) {
        if (!jsonObject.has("Plan")) {
            return null;
        }
        return findNodeRecursive(jsonObject.getAsJsonObject("Plan"), nodeType);
    }

    public static JsonPlan getJsonPlan(String filePath) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try (FileReader reader = new FileReader(filePath)) {
            JsonPlan jsonPlan = gson.fromJson(reader, JsonPlan.class);
            System.out.println(jsonPlan);
            return jsonPlan;
        } catch (Exception e) {
            throw new RuntimeException("Error reading json file: " + filePath, e);
        }
    }
}
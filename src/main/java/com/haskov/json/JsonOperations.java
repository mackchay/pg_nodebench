package com.haskov.json;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.postgresql.util.PGobject;

import java.util.ArrayList;
import java.util.List;

import static com.haskov.bench.V2.selectColumn;

public class JsonOperations {

    public static JsonObject explainResultsJson(String sql, Object... binds) {
        List<PGobject> pGobjectList = selectColumn("explain (analyze, verbose, buffers, costs off, format json) " + sql, binds);
        Gson gson = new Gson();
        JsonArray jsonArray = gson.fromJson(pGobjectList.get(0).getValue(), JsonArray.class);
        return jsonArray.get(0).getAsJsonObject();
    }

    public static List<JsonPlan> getAllPlans(JsonObject jsonObject) {
        return getAllPlansRecursive(jsonObject, new ArrayList<>());
    }

    private static List<JsonPlan> getAllPlansRecursive(JsonObject jsonObject, List<JsonPlan> list) {
        list.add(new JsonPlan(jsonObject));
        if (jsonObject.getAsJsonArray("Plans") != null) {
            JsonArray jsonArray = jsonObject.getAsJsonArray("Plans");
            for (JsonElement jsonElement : jsonArray) {
                return getAllPlansRecursive(jsonElement.getAsJsonObject(), list);
            }
        }
        return list;
    }

    private static JsonPlan findNodeRecursive(JsonObject jsonObject, String nodeType, String parameterKey,
                                              String parameterData) {
        if (nodeType.equals(jsonObject.get("Node Type").getAsString()) && jsonObject.has(parameterKey)
                && parameterData.equals(jsonObject.get(parameterKey).getAsString())) {
            return new JsonPlan(jsonObject);
        }

        if (jsonObject.has("Plans")) {
            JsonArray jsonArray = jsonObject.getAsJsonArray("Plans");

            for (JsonElement jsonElement : jsonArray) {
                JsonPlan result = findNodeRecursive(jsonElement.getAsJsonObject(), nodeType, parameterKey, parameterData);

                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    private static JsonPlan findNodeRecursive(JsonObject jsonObject, String nodeType) {
        if (nodeType.equals(jsonObject.get("Node Type").getAsString())) {
            return new JsonPlan(jsonObject);
        }

        if (jsonObject.has("Plans")) {
            JsonArray jsonArray = jsonObject.getAsJsonArray("Plans");
            for (JsonElement jsonElement : jsonArray) {
                JsonPlan result = findNodeRecursive(jsonElement.getAsJsonObject(), nodeType);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    public static JsonPlan findNode(JsonObject jsonObject, String nodeType, String parameterKey, String parameterData) {
        if (!jsonObject.has("Plan")) {
            return null;
        }
        return findNodeRecursive(jsonObject.getAsJsonObject("Plan"), nodeType, parameterKey, parameterData);
    }

    public static JsonPlan findNode(JsonObject jsonObject, String nodeType) {
        if (!jsonObject.has("Plan")) {
            return null;
        }
        return findNodeRecursive(jsonObject.getAsJsonObject("Plan"), nodeType);
    }

}
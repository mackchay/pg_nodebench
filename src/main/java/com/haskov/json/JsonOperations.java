package com.haskov.json;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.postgresql.util.PGobject;

import java.util.ArrayList;
import java.util.List;

import static com.haskov.bench.V2.select;

public class JsonOperations {

    public static JsonObject explainResultsJson(String sql, Object... binds) {
        List<PGobject> pGobjectList = select("explain (analyze, verbose, buffers, costs off, format json) " + sql, binds);
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

    //TODO: add NodeType and parameters
    private static JsonPlan findPlanElementRecursive(JsonObject jsonObject, String key, String planElement) {
        if (jsonObject.has(key) && planElement.equals(jsonObject.get(key).getAsString())) {
            return new JsonPlan(jsonObject);
        }

        if (jsonObject.has("Plans")) {
            JsonArray jsonArray = jsonObject.getAsJsonArray("Plans");

            for (JsonElement jsonElement : jsonArray) {
                JsonPlan result = findPlanElementRecursive(jsonElement.getAsJsonObject(), key, planElement);

                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    //TODO: add NodeType and parameters
    public static JsonPlan findPlanElement(JsonObject jsonObject, String key, String element) {
        if (jsonObject.has("Plan")) {
            return null;
        }
        return findPlanElementRecursive(jsonObject.getAsJsonObject("Plan"), key, element);
    }

}
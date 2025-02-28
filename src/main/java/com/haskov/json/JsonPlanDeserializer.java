package com.haskov.json;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Кастомный десериализатор
public class JsonPlanDeserializer implements JsonDeserializer<JsonPlan> {
    @Override
    public JsonPlan deserialize(JsonElement jsonElement, Type type,
                                JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        JsonPlan plan = new JsonPlan();

        if (jsonObject.has("Node Type")) {
            plan.setNodeType(jsonObject.get("Node Type").getAsString());
            jsonObject.remove("Node Type");
        }

        if (jsonObject.has("Plans")) {
            plan.setPlans(context.deserialize(jsonObject.get("Plans"), new TypeToken<List<JsonPlan>>() {}.getType()));
            jsonObject.remove("Plans");
        }

        // Оставшиеся поля записываем в params
        Map<String, String> paramsMap = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            paramsMap.put(entry.getKey(), context.deserialize(entry.getValue(), String.class));
        }
        plan.setParams(paramsMap);

        return plan;
    }

}


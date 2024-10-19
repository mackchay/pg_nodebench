package com.haskov.json;

import com.google.gson.JsonObject;
import lombok.Getter;

public class JsonPlan {
    private final String nodeType;
    @Getter
    private final JsonObject json;

    JsonPlan(JsonObject json) {
        this.nodeType = json.get("Node Type").getAsString();
        this.json = json;
    }

    public String getNodeType() {
        return nodeType;
    }

}
package com.haskov.json;

import com.google.gson.JsonObject;
import lombok.Getter;

@Getter
public class PgJsonPlan {
    private final String nodeType;
    private final JsonObject json;

    public PgJsonPlan(JsonObject json) {
        this.nodeType = json.get("Node Type").getAsString();
        this.json = json;
    }

}
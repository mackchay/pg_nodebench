package com.haskov.json;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
public class JsonPlan {
    @SerializedName("Node Type")
    private String nodeType;

    @SerializedName("Plans")
    private List<JsonPlan> plans;

    @Setter
    private Map<String, String> params = new HashMap<>();

    @Override
    public String toString() {
        return "Plan{" +
                "nodeType='" + nodeType + '\'' +
                ", plans=" + plans +
                ", params=" + params +
                '}';
    }
}

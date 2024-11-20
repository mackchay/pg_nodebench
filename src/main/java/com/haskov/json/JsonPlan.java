package com.haskov.json;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class JsonPlan {
    @SerializedName("Node Type")
    private String nodeType;

    @SerializedName("Plans")
    private List<JsonPlan> plans;

    @Override
    public String toString() {
        return "Plan{" +
                "nodeType='" + nodeType + '\'' +
                ", plans=" + plans +
                '}';
    }
}

package com.haskov;

import com.haskov.json.JsonPlan;
import com.haskov.json.PgJsonPlan;
import com.haskov.nodes.NodeTree;

import com.haskov.types.TableBuildResult;
import lombok.Getter;

import java.util.List;

public class PlanAnalyzer {
    private final long tableSize;
    private final JsonPlan plan;
    @Getter
    private final NodeTree root;

    public PlanAnalyzer(long tableSize, JsonPlan plan) {
        this.tableSize = tableSize;
        this.plan = plan;
        this.root = new NodeTree(plan);
    }

    public List<TableBuildResult> prepareTables() {
        return root.createTables(tableSize);
    }

    public boolean comparePlans(PgJsonPlan pgJsonPlan) {
        return comparePlans(plan, pgJsonPlan);
    }

    private static boolean comparePlans(JsonPlan jsonPlan, PgJsonPlan pgJsonPlan) {
        if (jsonPlan == null && pgJsonPlan == null) {
            return true;
        }
        if (jsonPlan == null || pgJsonPlan == null) {
            return false;
        }
        if (jsonPlan.getPlans() == null && pgJsonPlan.getJson().getAsJsonArray("Plans") == null) {
            return jsonPlan.getNodeType().equals(pgJsonPlan.getNodeType().replace(" ", ""));
        }
        if (jsonPlan.getPlans() == null || pgJsonPlan.getJson().getAsJsonArray("Plans") == null) {
            return false;
        }
        if (jsonPlan.getPlans().size() != pgJsonPlan.getJson().getAsJsonArray("Plans").size()) {
            return false;
        }
        int size = jsonPlan.getPlans().size();
        for (int i = 0; i < size; i++) {
            if (!comparePlans(
                    jsonPlan.getPlans().get(size - i - 1),
                    new PgJsonPlan(
                            pgJsonPlan.getJson().getAsJsonArray("Plans").get(i).getAsJsonObject()
                    )
            )) {
                return false;
            }
        }
        return true;
    }

}

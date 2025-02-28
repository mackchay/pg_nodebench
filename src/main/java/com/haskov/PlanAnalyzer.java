package com.haskov;

import com.haskov.json.JsonPlan;
import com.haskov.json.PgJsonPlan;
import com.haskov.nodes.NodeTree;

import com.haskov.types.TableBuildResult;
import lombok.Getter;

import java.util.List;
import java.util.Map;

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

    /**
     * Сравнение исхождного плана в json-файле и реального полученного из EXPLAIN
     * @param jsonPlan исходный план запроса
     * @param pgJsonPlan реальный план запроса
     * @return совпали планы или нет?
     */
    private static boolean comparePlans(JsonPlan jsonPlan, PgJsonPlan pgJsonPlan) {
        //Если два планы пустые, то планы равны
        if (jsonPlan == null && pgJsonPlan == null) {
            return true;
        }

        //Если один из планов null, а другой - нет, то планы не равны
        if (jsonPlan == null || pgJsonPlan == null) {
            return false;
        }

        //Если узлы не совпадают по типу, то планы не равны
        if (!jsonPlan.getNodeType().equals(pgJsonPlan.getNodeType().replace(" ", ""))) {
            return false;
        }


        //Если узел в одном плане имеет узлы потомков а в другом - нет, то планы не равны
        if (jsonPlan.getPlans() == null ^ pgJsonPlan.getJson().getAsJsonArray("Plans") == null) {
            return false;
        }

        //Если текущий узел что в исходном, что в реальном планах не имеет потомков узлов,
        // то сравнение закончено и они равны
        if (jsonPlan.getPlans() == null && pgJsonPlan.getJson().getAsJsonArray("Plans") == null) {
            return true;
        }

        Map<String, String> params = jsonPlan.getParams();
        for (String key : params.keySet()) {
            if (!params.get(key).equals(pgJsonPlan.getJson().get(key).getAsString())) {
                return false;
            }
        }

        //Если узел в одном плане имеет X узлов потомков а в другом - Y, то планы не равны
        if (jsonPlan.getPlans().size() != pgJsonPlan.getJson().getAsJsonArray("Plans").size()) {
            return false;
        }

        int size = jsonPlan.getPlans().size();

        // Рекурсия по деревьям планов.
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

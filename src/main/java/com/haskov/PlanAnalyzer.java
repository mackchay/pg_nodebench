package com.haskov;

import com.haskov.json.JsonPlan;
import com.haskov.json.PgJsonPlan;
import com.haskov.nodes.NodeTree;

import com.haskov.types.TableBuildResult;
import lombok.Getter;

import java.util.*;

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
        if (!jsonPlan.getNodeType().equals(pgJsonPlan.getNodeType().replace(" ", ""))) {
            return false;
        }

        if (jsonPlan.getPlans() == null ^ pgJsonPlan.getJson().getAsJsonArray("Plans") == null) {
            return false;
        }
        if (jsonPlan.getPlans() == null && pgJsonPlan.getJson().getAsJsonArray("Plans") == null) {
            return true;
        }

        Map<String, String> params = jsonPlan.getParams();
        for (String key : params.keySet()) {
            if (!params.get(key).equals(pgJsonPlan.getJson().get(key).getAsString())) {
                return false;
            }
        }

        List<JsonPlan> leftPlans = jsonPlan.getPlans();
        List<PgJsonPlan> rightPlans = new ArrayList<>();
        pgJsonPlan.getJson().getAsJsonArray("Plans").forEach(plan ->
                rightPlans.add(new PgJsonPlan(plan.getAsJsonObject()))
        );

        return compareUnorderedPlans(leftPlans, rightPlans);
    }

    /**
     * Сравнивает список подузлов без учета порядка (для Hash Join и Merge Join).
     */
    private static boolean compareUnorderedPlans(List<JsonPlan> leftPlans, List<PgJsonPlan> rightPlans) {
        if (leftPlans.size() != rightPlans.size()) {
            return false;
        }

        // Генерируем все возможные перестановки правого списка
        List<List<PgJsonPlan>> permutations = generatePermutations(rightPlans);

        for (List<PgJsonPlan> permutedRightPlans : permutations) {
            boolean allMatch = true;
            for (int i = 0; i < leftPlans.size(); i++) {
                if (!comparePlans(leftPlans.get(i), permutedRightPlans.get(i))) {
                    allMatch = false;
                    break;
                }
            }
            if (allMatch) {
                return true;
            }
        }
        return false;
    }

    /**
     * Сравнивает список подузлов с сортировкой (для Append, Merge Append).
     */
    private static boolean compareOrderedPlans(List<JsonPlan> leftPlans, List<PgJsonPlan> rightPlans) {
        if (leftPlans.size() != rightPlans.size()) {
            return false;
        }

        leftPlans.sort(Comparator.comparing(JsonPlan::getNodeType));
        rightPlans.sort(Comparator.comparing(p -> p.getJson().get("Node Type").getAsString()));

        for (int i = 0; i < leftPlans.size(); i++) {
            if (!comparePlans(leftPlans.get(i), rightPlans.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Генерирует все возможные перестановки списка.
     */
    private static <T> List<List<T>> generatePermutations(List<T> list) {
        List<List<T>> result = new ArrayList<>();
        permute(list, 0, result);
        return result;
    }

    private static <T> void permute(List<T> arr, int k, List<List<T>> result) {
        if (k == arr.size()) {
            result.add(new ArrayList<>(arr));
        } else {
            for (int i = k; i < arr.size(); i++) {
                Collections.swap(arr, i, k);
                permute(arr, k + 1, result);
                Collections.swap(arr, i, k);
            }
        }
    }


}

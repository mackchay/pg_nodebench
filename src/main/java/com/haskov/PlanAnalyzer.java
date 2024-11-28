package com.haskov;

import com.haskov.json.JsonPlan;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeFactory;
import com.haskov.nodes.scans.Scan;
import com.haskov.types.QueryNodeData;

import java.util.ArrayList;

public class PlanAnalyzer {
    private final long tableSize;
    private final JsonPlan plan;

    public PlanAnalyzer(long tableSize, JsonPlan plan) {
        this.tableSize = tableSize;
        this.plan = plan;
    }

    public String buildQuery() {
        return buildQueryRecursive(new QueryNodeData(
                new ArrayList<>(), new QueryBuilder(), tableSize
        ), plan).getQueryBuilder().build();
    }

    private static QueryNodeData buildQueryRecursive(QueryNodeData data, JsonPlan plan) {
        if (plan == null) {
            return data;
        }
        if (plan.getPlans() == null || plan.getPlans().isEmpty()) {
            Node node = NodeFactory.createNode(plan.getNodeType());
            if (data.getTables().isEmpty()) {
                data.setTables(node.prepareTables(data.getTableSize()));
            }
            data.setQueryBuilder(node.buildQuery(data.getTables(), data.getQueryBuilder()));
            return data;
        } else {
            Node node = NodeFactory.createNode(plan.getNodeType());
            if (node.getClass().isAnnotationPresent(Scan.class)) {
                throw new RuntimeException("Scan node should be leaf!");
            }

            int iterator = 1;
            for (JsonPlan nodePlan : plan.getPlans()) {
                buildQueryRecursive(data, nodePlan);
                if (iterator != plan.getPlans().size() || plan.getPlans().size() == 1) {
                    data.setQueryBuilder(node.buildQuery(data.getTables(), data.getQueryBuilder()));
                }
                if (plan.getPlans().size() > 1) {
                    data.setTables(new ArrayList<>());
                }
                iterator++;
            }
            return data;
        }
    }
}

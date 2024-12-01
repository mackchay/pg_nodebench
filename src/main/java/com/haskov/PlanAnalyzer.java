package com.haskov;

import com.haskov.json.JsonPlan;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeFactory;
import com.haskov.nodes.joins.Join;
import com.haskov.nodes.scans.Scan;
import com.haskov.tables.TableBuilder;
import com.haskov.types.QueryNodeData;
import com.haskov.types.TableBuildResult;

import java.util.ArrayList;
import java.util.List;

public class PlanAnalyzer {
    private final long tableSize;
    private final JsonPlan plan;
    private List<TableBuildResult> tableBuildResults;

    public PlanAnalyzer(long tableSize, JsonPlan plan) {
        this.tableSize = tableSize;
        this.plan = plan;
    }

    public String buildQuery() {
        return buildQueryRecursive(new QueryNodeData(
                new ArrayList<>(tableBuildResults),
                new QueryBuilder(),
                tableSize
        ), plan).getQueryBuilder().build();
    }

    public List<TableBuildResult> prepareTables() {
        tableBuildResults = prepareTablesRecursive(new QueryNodeData(
                new ArrayList<>(),
                new QueryBuilder(),
                tableSize
        ), plan);
        return tableBuildResults;
    }

    private static List<TableBuildResult> prepareTablesRecursive(QueryNodeData data, JsonPlan plan) {
        if (plan == null) {
            return data.getTableBuildDataList();
        }
        if (plan.getPlans() == null || plan.getPlans().isEmpty()) {
            Node node = NodeFactory.createNode(plan.getNodeType());
            if (!node.getClass().isAnnotationPresent(Scan.class)) {
                throw new RuntimeException("Only scan or result node should be leaf!");
            }
            data.getTableBuildDataList().add(node.prepareTables(data.getTableSize()));
            return data.getTableBuildDataList();
        }

        Node node = NodeFactory.createNode(plan.getNodeType());
        if (node.getClass().isAnnotationPresent(Scan.class)) {
            throw new RuntimeException("Scan node should be leaf!");
        }

        for (JsonPlan nodePlan : plan.getPlans()) {
            prepareTablesRecursive(data, nodePlan);
        }

        if (node.getClass().isAnnotationPresent(Join.class)) {
            TableBuilder.addForeignKey(
                    data.getTableBuildDataList().getFirst().tableName(),
                    data.getTableBuildDataList().getLast().tableName()
            );
        }

        return data.getTableBuildDataList();
    }

    private static QueryNodeData buildQueryRecursive(QueryNodeData data, JsonPlan plan) {
        if (plan == null) {
            return data;
        }
        Node node = NodeFactory.createNode(plan.getNodeType());
        if (plan.getPlans() == null || plan.getPlans().isEmpty()) {
            data.setQueryBuilder(node.buildQuery(
                    List.of(data.getTableBuildDataList().getFirst().tableName()),
                    data.getQueryBuilder())
            );
            return data;
        }

        if (node.getClass().isAnnotationPresent(Scan.class)) {
            throw new RuntimeException("Scan node should be leaf!");
        }

        int iterator = 1;
        for (JsonPlan nodePlan : plan.getPlans()) {
            buildQueryRecursive(data, nodePlan);
            if (plan.getPlans().size() != iterator || plan.getPlans().size() == 1) {
                data.setQueryBuilder(node.buildQuery(
                        data.getTableBuildDataList().stream()
                                .map(TableBuildResult::tableName).toList(),
                        data.getQueryBuilder()
                ));
            }
            if (plan.getPlans().size() != 1) {
                data.getTableBuildDataList().removeFirst();
            }
            iterator++;

        }
        return data;
    }
}

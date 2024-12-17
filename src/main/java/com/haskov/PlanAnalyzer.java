package com.haskov;

import com.haskov.json.JsonPlan;
import com.haskov.json.PgJsonPlan;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeFactory;
import com.haskov.nodes.NodeTree;
import com.haskov.nodes.joins.Join;
import com.haskov.nodes.scans.Scan;
import com.haskov.types.QueryNodeData;
import com.haskov.types.TableBuildResult;

import java.util.ArrayList;
import java.util.List;

public class PlanAnalyzer {
    private final long tableSize;
    private final JsonPlan plan;
    private final NodeTree nodeTree;
    private List<TableBuildResult> tableBuildResults;

    public PlanAnalyzer(long tableSize, JsonPlan plan) {
        this.tableSize = tableSize;
        this.plan = plan;
        this.nodeTree = new NodeTree(plan);
    }

//    public String buildQuery() {
//        return buildQueryRecursive(new QueryNodeData(
//                new ArrayList<>(tableBuildResults),
//                new QueryBuilder(),
//                tableSize
//        ), plan).getQueryBuilder().build();
//    }

    public String buildQuery() {
        nodeTree.prepareQuery();
        return nodeTree.buildQuery(new QueryBuilder()).build();
    }

//    public List<TableBuildResult> prepareTables() {
//        tableBuildResults = prepareTablesRecursive(new QueryNodeData(
//                new ArrayList<>(),
//                new QueryBuilder(),
//                tableSize
//        ), plan);
//        return tableBuildResults;
//    }

    public List<TableBuildResult> prepareTables() {
        tableBuildResults = nodeTree.createTables(tableSize);
        return tableBuildResults;
    }

    private static List<TableBuildResult> prepareTablesRecursive(QueryNodeData data, JsonPlan plan) {
        if (plan == null) {
            return data.getTableBuildDataList();
        }
        if (plan.getPlans() == null || plan.getPlans().isEmpty()) {
            Node node = NodeFactory.createNode(plan.getNodeType());
            if (!(node instanceof Scan scanNode)) {
                throw new RuntimeException("Only scan or result node should be leaf!");
            }
            data.getTableBuildDataList().add(scanNode.createTable(data.getTableSize()));
            return data.getTableBuildDataList();
        }

        Node node = NodeFactory.createNode(plan.getNodeType());
        if (node instanceof Scan) {
            throw new RuntimeException("Scan node should be leaf!");
        }

        for (JsonPlan nodePlan : plan.getPlans()) {
            prepareTablesRecursive(data, nodePlan);
        }

        if (node instanceof Join joinNode) {
            data.getTableBuildDataList().getLast().sqlScripts().addAll(
                    joinNode.prepareJoinTable(
                            data.getTableBuildDataList().getFirst().tableName(),
                            data.getTableBuildDataList().getLast().tableName()
                    ).sqlScripts()
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
                    data.getQueryBuilder())
            );
            return data;
        }

        if (node instanceof Scan) {
            throw new RuntimeException("Scan node should be leaf!");
        }

        int iterator = 1;
        for (JsonPlan nodePlan : plan.getPlans()) {
            buildQueryRecursive(data, nodePlan);
            if (plan.getPlans().size() != iterator || plan.getPlans().size() == 1) {
                data.setQueryBuilder(node.buildQuery(
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

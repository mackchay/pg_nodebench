package com.haskov;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.json.JsonOperations;
import com.haskov.json.JsonPlan;
import com.haskov.json.PgJsonPlan;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeFactory;
import com.haskov.nodes.scans.Scan;
import com.haskov.types.QueryNodeData;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static com.haskov.json.JsonOperations.explainResultsJson;
import static com.haskov.json.JsonOperations.findNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;


public class TestBuildQueryPlan {

    @Test
    public void testAggregate() {
        JsonPlan jsonPlan = getJsonPlan("aggregate.json");
        for (int i = 0; i < 1000; i++) {
            QueryNodeData q = buildQuery(new QueryNodeData(
                            new ArrayList<>(),
                            new QueryBuilder(),
                            1000
                    ), jsonPlan
            );
            String query = q.getQueryBuilder().build();
            System.out.println(query);
            //V2.explain(LoggerFactory.getLogger("TestBuildQueryPlan"), query);
            JsonObject resultsJson = explainResultsJson(query);
            PgJsonPlan pgJsonPlan1 = findNode(resultsJson, "Seq Scan");
            PgJsonPlan pgJsonPlan2 = findNode(resultsJson, "Aggregate");
            Assert.assertNotEquals(null, pgJsonPlan1);
            Assert.assertNotEquals(null, pgJsonPlan2);
        }
    }

    @Test
    public void testWrongAggregate() {
        JsonPlan plan = getJsonPlan("wrong_aggregate.json");
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            QueryNodeData q = buildQuery(new QueryNodeData(
                            new ArrayList<>(),
                            new QueryBuilder(),
                            1000
                    ), plan
            );
        });
        assertEquals("Aggregate requires a select columns: requires Scan or Result.", exception.getMessage());
    }

    @Test
    public void testWrongScan() {
        JsonPlan plan = getJsonPlan("wrong_scan.json");
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            QueryNodeData q = buildQuery(new QueryNodeData(
                            new ArrayList<>(),
                            new QueryBuilder(),
                            1000
                    ), plan
            );
        });
        assertEquals("Scan node should be leaf!", exception.getMessage());
    }

    @Test
    public void testAppend() {
        JsonPlan plan = getJsonPlan("append.json");
        for (int i = 0; i < 1000; i++) {
            QueryNodeData q = buildQuery(new QueryNodeData(
                            new ArrayList<>(),
                            new QueryBuilder(),
                            10000
                    ), plan
            );
            String query = q.getQueryBuilder().build();
            System.out.println(query);
            //V2.explain(LoggerFactory.getLogger("TestBuildQueryPlan"), query);
            JsonObject resultsJson = explainResultsJson(query);
            PgJsonPlan pgJsonPlan1 = findNode(resultsJson, "Seq Scan");
            PgJsonPlan pgJsonPlan2 = findNode(resultsJson, "Append");
            PgJsonPlan pgJsonPlan3 = findNode(resultsJson, "Index Only Scan");
            Assert.assertNotEquals(null, pgJsonPlan1);
            Assert.assertNotEquals(null, pgJsonPlan2);
            Assert.assertNotEquals(null, pgJsonPlan3);
        }
    }


// Helpful functions

    @Test
    public void testBuildQueryPlan() {
        List<String> nodes = new ArrayList<>(
                List.of("SeqScan", "Aggregate", "Append", "SeqScan", "BitmapIndexScan", "BitmapHeapScan")
        );
    }

    private void initDB() {
        String argArray = "-h localhost -n NestedLoop -S 1000";
        String[] args = List.of(argArray.split(" ")).toArray(new String[0]);
        Configuration conf = Cmd.args(args);
        V2.init(conf);
    }

    private JsonPlan getJsonPlan(String resourceName) {
        initDB();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try (InputStream inputStream = JsonPlan.class
                .getClassLoader()
                .getResourceAsStream(resourceName)) {

            if (inputStream == null) {
                throw new RuntimeException("File not found in resources!");
            }

            JsonPlan jsonPlan = gson.fromJson(new InputStreamReader(inputStream), JsonPlan.class);
            System.out.println(jsonPlan);
            return jsonPlan;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static QueryNodeData buildQuery(QueryNodeData data, JsonPlan plan) {
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
                buildQuery(data, nodePlan);
                String query = data.getQueryBuilder().build();
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

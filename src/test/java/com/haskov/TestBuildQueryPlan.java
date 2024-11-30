package com.haskov;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.json.JsonPlan;
import com.haskov.json.PgJsonPlan;
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
        long tableSize = 1000;
        JsonPlan jsonPlan = getJsonPlan("testplans/aggregate.json", tableSize);
        PlanAnalyzer analyzer = new PlanAnalyzer(tableSize, jsonPlan);
        for (int i = 0; i < 1000; i++) {
            String query = analyzer.buildQuery();
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
        long tableSize = 1000;
        JsonPlan plan = getJsonPlan("wrong_aggregate.json", tableSize);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            PlanAnalyzer analyzer = new PlanAnalyzer(tableSize, plan);
            String q = analyzer.buildQuery();
        });
        assertEquals("Aggregate requires a select columns: requires Scan or Result.", exception.getMessage());
    }

    @Test
    public void testWrongScan() {
        long tableSize = 1000;
        JsonPlan plan = getJsonPlan("wrong_scan.json", tableSize);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            PlanAnalyzer analyzer = new PlanAnalyzer(tableSize, plan);
            String q = analyzer.buildQuery();
        });
        assertEquals("Scan node should be leaf!", exception.getMessage());
    }

    @Test
    public void testAppend() {
        long tableSize = 1000;
        JsonPlan plan = getJsonPlan("testplans/append.json", tableSize);
        PlanAnalyzer analyzer = new PlanAnalyzer(tableSize, plan);
        for (int i = 0; i < 1000; i++) {
            String query = analyzer.buildQuery();
            System.out.println(query);
            V2.explain(LoggerFactory.getLogger("TestBuildQueryPlan"), query);
            JsonObject resultsJson = explainResultsJson(query);
            PgJsonPlan pgJsonPlan1 = findNode(resultsJson, "Seq Scan");
            PgJsonPlan pgJsonPlan2 = findNode(resultsJson, "Append");
            PgJsonPlan pgJsonPlan3 = findNode(resultsJson, "Index Only Scan");
            PgJsonPlan pgJsonPlan4 = findNode(resultsJson, "Bitmap Index Scan");
            Assert.assertNotEquals(null, pgJsonPlan1);
            Assert.assertNotEquals(null, pgJsonPlan2);
            Assert.assertNotEquals(null, pgJsonPlan3);
            Assert.assertNotEquals(null, pgJsonPlan4);
        }
    }

    @Test
    public void testBitmapScan() {
        long tableSize = 1000;
        JsonPlan plan = getJsonPlan("aggregate_bitmapscan.json", tableSize);
        PlanAnalyzer analyzer = new PlanAnalyzer(tableSize, plan);
        for (int i = 0; i < 1000; i++) {
            String query = analyzer.buildQuery();
            System.out.println(query);
            //V2.explain(LoggerFactory.getLogger("TestBuildQueryPlan"), query);
            JsonObject resultsJson = explainResultsJson(query);
            PgJsonPlan pgJsonPlan1 = findNode(resultsJson, "Aggregate");
            PgJsonPlan pgJsonPlan2 = findNode(resultsJson, "Bitmap Heap Scan");
            PgJsonPlan pgJsonPlan3 = findNode(resultsJson, "Bitmap Index Scan");
            Assert.assertNotEquals(null, pgJsonPlan1);
            Assert.assertNotEquals(null, pgJsonPlan2);
            Assert.assertNotEquals(null, pgJsonPlan3);
        }
    }

    @Test
    public void testWrongBitmapScan() {
        long tableSize = 1000;
        JsonPlan plan = getJsonPlan("wrong_bitmapscan.json", tableSize);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            PlanAnalyzer analyzer = new PlanAnalyzer(tableSize, plan);
            String q = analyzer.buildQuery();
        });
        assertEquals("Column names must be specified", exception.getMessage());
    }




// Helpful functions

    @Test
    public void testBuildQueryPlan() {
        List<String> nodes = new ArrayList<>(
                List.of("SeqScan", "Aggregate", "Append", "SeqScan", "BitmapIndexScan", "BitmapHeapScan")
        );
    }

    private void initDB(long size) {
        String argArray = "-h localhost -j testplans/bitmapscan.json -S " + size;
        String[] args = List.of(argArray.split(" ")).toArray(new String[0]);
        Configuration conf = Cmd.args(args);
        V2.init(conf);
    }

    private JsonPlan getJsonPlan(String resourceName, long size) {
        initDB(size);
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
}

package com.haskov.test;

import com.google.gson.JsonObject;
import com.haskov.bench.v2.Results;
import com.haskov.json.PgJsonPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static com.haskov.bench.V2.*;
import static com.haskov.json.JsonOperations.explainResultsJson;
import static com.haskov.json.JsonOperations.findNode;

public class TestUtils {
    private final static String NODE_TYPE_JSON_KEY = "Node Type";

    private final static Logger logger = LoggerFactory.getLogger(TestUtils.class);

    public static void testQuery(String query, Object... binds) {
        logger.info("Testing query: {}", query);
        Results parallelState = parallel((state) -> sql(query, binds));
    }

    public static void testQueries(String[] queries) {
        Random random = new Random();
        Results parallelState = parallel((state -> {
            sql(queries[(int) state.iterationsDone % queries.length]);
        }
        ));

    }

    //Test queries for node types.
    public static void testQueriesOnNode(String[] queries, String expectedNodeType) {
        expectedNodeType = String.join(" ", expectedNodeType.split("(?=[A-Z])"));
        sql("SET max_parallel_workers_per_gather = 0");
        for (String query : queries) {
            //explain(logger, query);
            JsonObject resultsJson = explainResultsJson(query);
            PgJsonPlan pgJsonPlan = findNode(resultsJson, expectedNodeType);
            if (pgJsonPlan == null) {
                explain(logger, query);
                throw new RuntimeException("Query: " + query + ". Could not find expected node " + expectedNodeType);
            }
            //TestUtils.testQuery(query);
        }
        //TestUtils.testQueries(queries);
    }

    //Test queries for node type and parameters
    public static void testQueriesOnNode(String[] queries, String expectedNodeType,
                                         String nodeParameter, String expectedNodeParameterData) {
        expectedNodeType = String.join(" ", expectedNodeType.split("(?=[A-Z])"));
        for (String query : queries) {
            explain(logger, query);
            JsonObject resultsJson = explainResultsJson(query);
            PgJsonPlan pgJsonPlan = findNode(resultsJson, expectedNodeType, nodeParameter, expectedNodeParameterData);
            if (pgJsonPlan == null) {
                throw new RuntimeException("Query: " + query + ". Could not find expected node: " + expectedNodeType +
                        " , expected node parameter " + nodeParameter + " and expected node parameter data "
                        + expectedNodeParameterData);
            }
            //TestUtils.testQuery(query);
        }
        TestUtils.testQueries(queries);
    }
}

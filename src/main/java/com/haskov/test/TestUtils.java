package com.haskov.test;

import com.google.gson.JsonObject;
import com.haskov.bench.v2.Results;
import com.haskov.json.JsonPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    //Test queries for node types.
    public static void testQueriesOnNode(String[] queries, String expectedNodeType) {
        expectedNodeType = String.join(" ", expectedNodeType.split("(?=[A-Z])"));
        for (String query : queries) {
            explain(logger, query);
            JsonObject resultsJson = explainResultsJson(query);
            JsonPlan jsonPlan = findNode(resultsJson, expectedNodeType);
            if (jsonPlan == null) {
                throw new RuntimeException("Query: " + query + ". Could not find expected node " + expectedNodeType);
            }
            TestUtils.testQuery(query);
        }
    }

    //Test queries for node type and parameters
    public static void testQueriesOnNode(String[] queries, String expectedNodeType,
                                         String nodeParameter, String expectedNodeParameterData) {
        expectedNodeType = String.join(" ", expectedNodeType.split("(?=[A-Z])"));
        for (String query : queries) {
            explain(logger, query);
            JsonObject resultsJson = explainResultsJson(query);
            JsonPlan jsonPlan = findNode(resultsJson, expectedNodeType, nodeParameter, expectedNodeParameterData);
            if (jsonPlan == null) {
                throw new RuntimeException("Query: " + query + ". Could not find expected node: " + expectedNodeType +
                        " , expected node parameter " + nodeParameter + " and expected node parameter data "
                        + expectedNodeParameterData);
            }
            TestUtils.testQuery(query);
        }
    }

}

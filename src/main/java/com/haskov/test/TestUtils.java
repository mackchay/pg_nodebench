package com.haskov.test;

import com.google.gson.JsonObject;
import com.haskov.bench.v2.Results;
import com.haskov.json.JsonPlan;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static com.haskov.bench.V2.*;
import static com.haskov.json.JsonOperations.explainResultsJson;
import static com.haskov.json.JsonOperations.findPlanElement;

public class TestUtils {
    private final static String NODE_TYPE_JSON_KEY = "Node Type";

    private final static Logger logger = LoggerFactory.getLogger(TestUtils.class);

    public static void testQuery(String query, Object... binds) {
        Results parallelState = parallel((state) -> sql(query, binds));
    }

    //Test queries for node types.
    public static void testQueriesOnNode(String[] queries, String expectedNodeType) {
        for (String query : queries) {
            explain(logger, query);
            JsonObject resultsJson = explainResultsJson(query);
            String actualNode = Objects.requireNonNull(findPlanElement(resultsJson, NODE_TYPE_JSON_KEY, expectedNodeType)).
                    getJson().get(NODE_TYPE_JSON_KEY).getAsString();
            TestUtils.testQuery(query);
        }
    }

    //Test queries for node type and parameters
    public static void testQueriesOnNode(String[] queries, String expectedNodeType,
                                         String nodeParameter, String expectedNodeParameterData) {
        for (String query : queries) {
            explain(logger, query);
            JsonObject resultsJson = explainResultsJson(query);
            JsonPlan jsonPlan = findPlanElement(resultsJson, NODE_TYPE_JSON_KEY, expectedNodeType);
            assert jsonPlan != null;
            String actualNodeType = jsonPlan.getPlanElement();
            String actualPlanElement = "";
            if (jsonPlan.getJson().has(nodeParameter)) {
                actualPlanElement = jsonPlan.getJson().get(nodeParameter).getAsString();
            }
            assertNodes(logger, query, expectedNodeType, actualNodeType);
            assertNodes(logger, query, nodeParameter, expectedNodeParameterData, actualPlanElement);
            TestUtils.testQuery(logger, query);
        }
    }

    private static void assertPlans(Logger logger, String query, String expectedPlanType, String actualPlanType) {
        try {
            Assert.assertEquals(expectedPlanType, actualPlanType);
            logger.info("Plan element check completed for {} plan in query: {}", expectedPlanType, query);
        } catch (AssertionError e) {
            logger.error("{} in query: {}", e, query);
            throw new RuntimeException(e);
        }
    }

    private static void assertNodes(Logger logger, String query, String planElementName,
                                    String expectedPlanElement, String actualPlanElement) {
        try {
            Assert.assertEquals(expectedPlanElement, actualPlanElement);
            logger.info("{} check completed for {} {} in query: {}", planElementName,
                    expectedPlanElement, planElementName, query);
        } catch (AssertionError e) {
            logger.error("{} in query: {}", e, query);
            throw new RuntimeException(e);
        }
    }

}

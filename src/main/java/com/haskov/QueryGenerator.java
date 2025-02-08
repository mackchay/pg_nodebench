package com.haskov;

import com.haskov.bench.V2;
import com.haskov.json.JsonOperations;
import com.haskov.json.JsonPlan;
import com.haskov.json.PgJsonPlan;
import com.haskov.nodes.NodeTree;
import com.haskov.types.TableBuildResult;

import java.util.ArrayList;
import java.util.List;

import static com.haskov.bench.V2.sql;

public class QueryGenerator {
    private NodeTree root;

    public QueryGenerator(NodeTree root) {
        this.root = root;
    }

    public List<String> generate(int queryCount) {

        List<String> queries = new ArrayList<>();
        sql("SET max_parallel_workers_per_gather = 0");
        for (int i = 0; i < queryCount; i++) {
            //sql("analyze");
            String query = buildQuery();
            queries.add(query);
        }

        return queries;
    }

    private String buildQuery() {
        root.prepareQuery();
        return root.buildQuery();
    }
}

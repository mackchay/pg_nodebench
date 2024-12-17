package com.haskov;

import com.haskov.bench.V2;
import com.haskov.json.JsonOperations;
import com.haskov.json.JsonPlan;
import com.haskov.json.PgJsonPlan;
import com.haskov.tables.DropTable;
import com.haskov.test.TestUtils;
import com.haskov.types.TableBuildResult;

import java.util.ArrayList;
import java.util.List;

import static com.haskov.bench.V2.sql;

public class QueryGenerator {

    public List<String> generate(long sizeOfTable, JsonPlan plan,
                                        int queryCount) {
        PlanAnalyzer analyzer = new PlanAnalyzer(sizeOfTable, plan);
        List<TableBuildResult> tableScripts = analyzer.prepareTables();
        List<String> queries = new ArrayList<>();
        sql("SET max_parallel_workers_per_gather = 0");
        for (int i = 0; i < queryCount; i++) {
            //sql("analyze");
            String query = analyzer.buildQuery();
            System.out.println(query);
            queries.add(query);
            //V2.explain(V2.log, query);
            if (!analyzer.comparePlans(new PgJsonPlan(
                    JsonOperations.explainResultsJson(query).getAsJsonObject("Plan")
            ))) {
                System.out.println(plan);
                V2.explain(V2.log, query);
                throw new RuntimeException("Query: " + query + " failed");
            };
        }
        ReportGenerator generator = new ReportGenerator();
        generator.generate(
                tableScripts.stream().map(TableBuildResult::sqlScripts)
                        .flatMap(List::stream).toList(), queries
        );
        return queries;
    }
}

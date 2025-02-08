package com.haskov;

import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.json.JsonOperations;
import com.haskov.json.JsonPlan;
import com.haskov.json.PgJsonPlan;
import com.haskov.types.TableBuildResult;

import java.util.List;

public class NodeBenchMaster {
    private long sizeOfTable;
    private JsonPlan plan;
    private int queryCount;

    public NodeBenchMaster(Configuration conf) {
        this.sizeOfTable = conf.tableSize;
        this.plan = conf.plan;
        this.queryCount = conf.queryCount;
    }

    public NodeBenchMaster(long sizeOfTable, JsonPlan plan,
                           int queryCount) {
        this.sizeOfTable = sizeOfTable;
        this.plan = plan;
        this.queryCount = queryCount;
    }

    public void start() {
        PlanAnalyzer analyzer = new PlanAnalyzer(sizeOfTable, plan);
        List<TableBuildResult> tableScripts = analyzer.prepareTables();
        QueryGenerator queryGenerator = new QueryGenerator(analyzer.getRoot());
        List<String> queries = queryGenerator.generate(queryCount);

        for (String query : queries) {
            if (!analyzer.comparePlans(new PgJsonPlan(
                    JsonOperations.explainResultsJson(query).getAsJsonObject("Plan")
            ))) {
                System.out.println(query);
                System.out.println(plan);
                V2.explain(V2.log, query);
                throw new RuntimeException("Invalid result query plan!");
            }
        }

        SQLScriptsOutput output = new SQLScriptsOutput();
        output.writeScripts(
                tableScripts.stream().map(TableBuildResult::sqlScripts)
                        .flatMap(List::stream).toList(), queries
        );
    }
}

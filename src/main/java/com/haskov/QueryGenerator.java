package com.haskov;

import com.haskov.json.JsonPlan;
import com.haskov.tables.DropTable;
import com.haskov.test.TestUtils;
import com.haskov.types.TableBuildResult;

import java.util.ArrayList;
import java.util.List;

public class QueryGenerator {

    public List<String> generate(long sizeOfTable, JsonPlan plan,
                                        int queryCount) {
        PlanAnalyzer analyzer = new PlanAnalyzer(sizeOfTable, plan);
        List<TableBuildResult> tableScripts = analyzer.prepareTables();
        List<String> queries = new ArrayList<>();
        for (int i = 0; i < queryCount; i++) {
            String query = analyzer.buildQuery();
            System.out.println(query);
            queries.add(query);
        }
        ReportGenerator generator = new ReportGenerator();
        generator.generate(
                tableScripts.stream().map(TableBuildResult::sqlScripts)
                        .flatMap(List::stream).toList(), queries
        );
        return queries;
    }
}

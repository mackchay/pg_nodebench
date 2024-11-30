package com.haskov;

import com.haskov.json.JsonPlan;
import com.haskov.tables.DropTable;
import com.haskov.test.TestUtils;

import java.util.ArrayList;
import java.util.List;

public class QueryGenerator {

    public List<String> generate(long sizeOfTable, JsonPlan plan,
                                        int queryCount) {
        PlanAnalyzer analyzer = new PlanAnalyzer(sizeOfTable, plan);
        analyzer.prepareTables();
        List<String> queries = new ArrayList<>();
        for (int i = 0; i < queryCount; i++) {
            String query = analyzer.buildQuery();
            System.out.println(query);
            queries.add(query);
        }
        return queries;
    }
}

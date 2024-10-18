package com.haskov.nodes;

import com.haskov.bench.V2;
import com.haskov.tables.DropTable;

import java.util.List;

public class indexScan implements Node{
    @Override
    public String buildQuery(List<String> tables) {
        return "";
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        DropTable.dropTable("test");
        V2.sql("create index if not exists test_index on test");
        V2.sql("create table test ( x integer )");
        V2.sql("insert into test (x) select generate_series(1, $1);", tableSize);
        return List.of("test");
    }
}

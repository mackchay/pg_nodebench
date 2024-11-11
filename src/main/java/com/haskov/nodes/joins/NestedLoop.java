package com.haskov.nodes.joins;

import com.haskov.nodes.Node;
import com.haskov.tables.DropTable;
import com.haskov.tables.TableBuilder;
import com.haskov.tables.TableData;

import java.util.ArrayList;
import java.util.List;

public class NestedLoop implements Node {

    @Override
    public String buildQuery(List<String> tables) {
        return "";
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        String tableName = "pg_nestedloop_parent";
        DropTable.dropTable(tableName);
        TableBuilder.createRandomTable(new TableData(
                tableName,
                new ArrayList<>(),
                5,
                tableSize,
                new ArrayList<>(),
                new ArrayList<>(),
                true
        ));
        return List.of();
    }
}

package com.haskov.nodes.joins;

import com.haskov.QueryBuilder;
import com.haskov.nodes.Node;
import com.haskov.tables.DropTable;
import com.haskov.tables.TableBuilder;
import com.haskov.types.TableData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.haskov.bench.V2.getColumnsAndTypes;

public class NestedLoop implements Node {

    @Override
    public String buildQuery(List<String> tables) {
        QueryBuilder qb = new QueryBuilder();
        Random random = new Random();

        //Expected at least 2 tables.
        int tableCount = Math.max(random.nextInt(tables.size()) + 1, 2);
        Collections.shuffle(tables);
        tables = tables.subList(0, tableCount);

        for (String table : tables) {
            List<String> column = new ArrayList<>(getColumnsAndTypes(table).keySet());
            int columnsCount = random.nextInt(column.size()) + 1;
            Collections.shuffle(column);
            qb.from(table);
            for (int j = 0; j < columnsCount; j++) {
                qb.addRandomWhere(table, column.get(j));
            }
        }

        //qb.join(tables.get(0), tables.get(1));

        return qb.build();
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        String parentName = "pg_nestedloop_parent";
        String childName = "pg_nestedloop_child";
        DropTable.dropTable(childName);
        DropTable.dropTable(parentName);

        List<String> tables = new ArrayList<>(List.of(parentName, childName));
        TableBuilder.createRandomTable(new TableData(
                parentName,
                new ArrayList<>(),
                5,
                tableSize,
                new ArrayList<>(List.of(true, true, true, true, true)),
                new ArrayList<>(),
                true
        ));
        TableBuilder.createRandomTable(new TableData(
                childName,
                new ArrayList<>(List.of(parentName)),
                5,
                tableSize,
                new ArrayList<>(List.of(true, true, true, true, true)),
                new ArrayList<>(),
                true
        ));
        Random random = new Random();
        int maxColumns = 30;
        for (int i = 0; i < 3; i++) {
            int size = random.nextInt(0, maxColumns);
            String tableName = "pg_nestedloop_" + i;
            tables.add(tableName);
            DropTable.dropTable(tableName);
            TableBuilder.createRandomTable(new TableData(
                    tableName,
                    new ArrayList<>(),
                    size,
                    tableSize,
                    getRandomBooleanList(size),
                    new ArrayList<>(),
                    random.nextBoolean()
            ));
        }
        return tables;
    }

    private List<Boolean> getRandomBooleanList(int size) {
        Random random = new Random();
        List<Boolean> list = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            list.add(random.nextDouble() < 0.5);
        }

        return list;
    }
}

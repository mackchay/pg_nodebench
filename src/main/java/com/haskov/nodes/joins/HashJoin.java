package com.haskov.nodes.joins;

import com.haskov.QueryBuilder;
import com.haskov.nodes.Node;
import com.haskov.tables.DropTable;
import com.haskov.types.JoinData;
import com.haskov.types.JoinType;
import com.haskov.types.TableBuildResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Join
public class HashJoin implements Node {

    @Override
    public String buildQuery(List<String> tables) {
        return "";
    }

    @Override
    public QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        return Node.super.buildQuery(tables, qb);
    }

    @Override
    public TableBuildResult prepareTables(Long tableSize) {
        Random random = new Random();
        int maxColumns = 30;
        int maxTables = 10;
        String baseTableName = "pg_hashjoin";
        List<String> tables = new ArrayList<>();
        for (int i = 0; i < maxTables; i++) {
            tables.add(baseTableName + "_" + i);
        }

        List<JoinData> joinTypes = generateTableScheme(tables);
        for (int i = 0; i < 3; i++) {
            int size = random.nextInt(0, maxColumns);
            String tableName = "pg_nestedloop_" + i;
            tables.add(tableName);
            DropTable.dropTable(tableName);
//            TableBuilder.createRandomTable(new TableData(
//                    tableName,
//                    new ArrayList<>(),
//                    size,
//                    tableSize,
//                    getRandomBooleanList(size),
//                    new ArrayList<>(),
//                    random.nextBoolean()
//            ));
        }
        return null;
    }

    private List<Boolean> getRandomBooleanList(int size) {
        Random random = new Random();
        List<Boolean> list = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            list.add(random.nextDouble() < 0.5);
        }

        return list;
    }

    private List<JoinData> generateTableScheme(List<String> tables) {
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("List of tables must have at least one table");
        }

        Random random = new Random();
        List<String> nodes = new ArrayList<>(List.of(tables.getFirst()));
        List<JoinData> joins = new ArrayList<>();
        for (String table : tables.subList(1, tables.size())) {
            int randInt = random.nextInt(nodes.size());
            List<JoinType> joinTypes = new ArrayList<>(List.of(JoinType.values()));
            joinTypes.remove(JoinType.CROSS);
            int randJoinType = random.nextInt(joinTypes.size());
            JoinType joinType = JoinType.class.getEnumConstants()[randJoinType];
            joins.add(new JoinData(nodes.get(randInt), table, joinType));
        }
        return joins;
    }
}

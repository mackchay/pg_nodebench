package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.nodes.Node;
import com.haskov.tables.DropTable;
import com.haskov.utils.SQLUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.haskov.bench.V2.sql;

@Scan
public class BitmapIndexScan implements Node {
    @Override
    public String buildQuery(List<String> tables) {
        return "";
    }

    @Override
    public QueryBuilder buildQuery(List<String> tables, QueryBuilder qb) {
        qb.from(tables.getFirst());
        return qb;
    }

    @Override
    public List<String> prepareTables(Long tableSize) {
        String tableName = "pg_bitmapscan";
        if (SQLUtils.getTableRowCount(tableName).equals(tableSize)) {
            sql("create index if not exists pg_bitmapscan_idx on " + tableName + "(x)");
            V2.sql("vacuum freeze analyze " + tableName);
            return new ArrayList<>(List.of(tableName));
        }
        DropTable.dropTable(tableName);
        List<Long> list1 = new ArrayList<>();
        for (long i = 1; i <= tableSize; i++) {
            list1.add(i);
        }
        Collections.shuffle(list1);

        sql("create table " + tableName + " (x integer, y integer, z integer)");

        StringBuilder query = new StringBuilder("insert into " + tableName + "(x, y, z) values ");
        for (int i = 0; i < list1.size(); i++) {
            query.append("(").append(list1.get(i)).append(",").append(list1.get(i)).append(",")
                    .append(list1.get(i)).append(")").append(",");
        }
        sql(query.delete(query.length() - 1, query.length()).toString());

        sql("create index if not exists pg_bitmapscan_idx on " + tableName + "(x)");

        V2.sql("vacuum freeze analyze " + tableName);
        return new ArrayList<>(List.of(tableName));
    }
}

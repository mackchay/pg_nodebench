package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.types.InsertType;
import com.haskov.types.TableBuildResult;
import com.haskov.types.TableIndexType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.haskov.bench.V2.getColumnsAndTypes;
import static com.haskov.tables.TableBuilder.buildRandomTable;

public class TidScan implements TableScan {
    private int columnsConditionsCount = 0;
    private String table = "";
    private List<String> columns = new ArrayList<>();
    private long tableSize;

    @Override
    public TableBuildResult initScanNode(Long tableSize) {
        TableBuildResult result = createTable(tableSize);
        table = result.tableName();
        this.tableSize = tableSize;
        columns = new ArrayList<>(getColumnsAndTypes(table).keySet());
        return result;
    }

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        Pair<Long, Long> tupleRange = getTuplesRange();
        qb.setMinMaxTuples(tupleRange.getLeft(), tupleRange.getRight());
        qb.from(table);

        Random random = new Random();
        int page = random.nextInt(2);
        int tuple = random.nextInt(2);
        qb.whereCTid(new ImmutablePair<>(page, tuple));
        for (int i = 0; i < columnsConditionsCount; i++) {
            qb.select(table + "." + columns.get(i));
        }
        return qb;
    }

    @Override
    public void prepareScanQuery() {
        Random random = new Random();
        columnsConditionsCount = random.nextInt(columns.size()) + 1;
        Collections.shuffle(columns);
    }

    public TableBuildResult createTable(Long tableSize) {
        String tableName = "pg_seqscan";
        return buildRandomTable(tableName, tableSize,
                InsertType.ASCENDING, TableIndexType.FULL_NON_INDEX);
    }

    @Override
    public Pair<Double, Double> getCosts(double sel) {
        return new ImmutablePair<>(0.0, 0.0);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return new ImmutablePair<>(0, 1);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        return new ImmutablePair<>(0L, tableSize);
    }

    @Override
    public List<String> getTables() {
        return List.of(table);
    }
}

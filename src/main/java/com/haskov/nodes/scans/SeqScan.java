package com.haskov.nodes.scans;

import com.haskov.QueryBuilder;
import com.haskov.costs.ScanCostCalculator;
import com.haskov.nodes.Node;
import com.haskov.types.InsertType;
import com.haskov.types.TableBuildResult;
import com.haskov.types.TableIndexType;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.haskov.bench.V2.getColumnsAndTypes;
import static com.haskov.tables.TableBuilder.buildRandomTable;

public class SeqScan implements Node, Scan {
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
    public String buildQuery() {
        return buildQuery(new QueryBuilder()).build();
    }

    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {
        Random random = new Random();
        qb.from(table);
        for (String column : columns.subList(0, columnsConditionsCount)) {
            qb.randomWhere(table, column);
        }
        return qb;
    }

    @Override
    public void prepareQuery() {
        Random random = new Random();
        columnsConditionsCount = random.nextInt(columns.size()) + 1;
        Collections.shuffle(columns);
    }

    @Override
    public long reCalculateMinTuple(long tuples) {
        double tmpSel = (double) tuples / tableSize;
        while (tableSize * Math.pow(tmpSel, columnsConditionsCount) < 2) {
            tmpSel *= 1.05;
        }
        return (long) (tableSize * tmpSel);
    }


    public TableBuildResult createTable(Long tableSize) {
        String tableName = "pg_seqscan";
        return buildRandomTable(tableName, tableSize,
                InsertType.ASCENDING, TableIndexType.FULL_NON_INDEX);
    }

    @Override
    public Pair<Double, Double> getCosts() {
        double totalCost = ScanCostCalculator.calculateSeqScanCost(table, columnsConditionsCount * 2);
        return new ImmutablePair<>(0.0, totalCost);
    }

    @Override
    public Pair<Integer, Integer> getConditions() {
        return new ImmutablePair<>(0, columnsConditionsCount);
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        return new ImmutablePair<>(0L, SQLUtils.getTableRowCount(table));
    }

    @Override
    public double getSel() {
        return 1;
    }
}

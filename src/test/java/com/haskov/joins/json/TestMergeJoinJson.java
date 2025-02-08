package com.haskov.joins.json;

import com.haskov.Cmd;
import com.haskov.QueryGenerator;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import org.junit.Test;

import java.util.List;

public class TestMergeJoinJson {
    private final static String expectedNodeType = "MergeJoin";
    private final static String filePath = "testplans/mergejoin.json";

    public void test(long size, int queryCount) {
        String argArray = "-h localhost -j " + filePath + " -S " + size + " -q " + queryCount;
        Configuration conf = Cmd.args(argArray.split(" "));
        V2.init(conf);
        QueryGenerator qb = new QueryGenerator();
        List<String> queries = qb.generate(conf.tableSize, conf.plan, conf.queryCount);
        V2.closeConnection();
    }

    @Test
    public void testMergeJoin() {
        test(200, 1000);
        test(800, 1000);
        test(1000, 800);
        test(10000, 500);
        test(100000, 50);
    }
}

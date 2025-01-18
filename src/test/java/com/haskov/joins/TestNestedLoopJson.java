package com.haskov.joins;

import com.haskov.Cmd;
import com.haskov.QueryGenerator;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import org.junit.Test;

import java.util.List;

public class TestNestedLoopJson {
    private final static String expectedNodeType = "NestedLoop";
    private final static String filePath = "testplans/nestedloop.json";

    public void test(long size, int queryCount) {
        String argArray = "-h localhost -j " + filePath + " -S " + size + " -q " + queryCount;
        Configuration conf = Cmd.args(argArray.split(" "));
        V2.init(conf);
        QueryGenerator qb = new QueryGenerator();
        List<String> queries = qb.generate(conf.tableSize, conf.plan, conf.queryCount);
    }

    @Test
    public void testNestedLoop() {
        test(800, 1000);
        test(1000, 800);
        test(10000, 500);
        test(100000, 100);
    }
}

package com.haskov.functions;

import com.haskov.Cmd;
import com.haskov.NodeBenchMaster;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import org.junit.Test;


public class TestAppendJson {
    private String filePath = "testplans/append_aggregate.json";

    public void test(long size, int queryCount) {
        String argArray = "-h localhost -j " + filePath + " -S " + size + " -q " + queryCount;
        Configuration conf = Cmd.args(argArray.split(" "));
        V2.init(conf);
        NodeBenchMaster master = new NodeBenchMaster(conf);
        master.start();
        V2.closeConnection();
    }

    @Test
    public void testAppend() {
        filePath = "testplans/append.json";
        test(1000, 1000);
        test(10000, 500);
        test(100000, 50);
    }

    @Test
    public void testAppendAggregate() {
        filePath = "testplans/append_aggregate.json";
        test(1000, 1000);
        test(10000, 500);
        test(100000, 50);
    }

    @Test
    public void testAppendAggregateSubquery() {
        filePath = "testplans/append_aggregate_subquery.json";
        test(1000, 1000);
        test(10000, 500);
        test(100000, 50);
    }

    @Test
    public void testAppendResult() {
        filePath = "testplans/append_result.json";
        test(1000, 1000);
        test(10000, 500);
        test(100000, 50);
    }
}

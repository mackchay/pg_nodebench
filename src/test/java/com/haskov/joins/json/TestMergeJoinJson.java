package com.haskov.joins.json;

import com.haskov.Cmd;
import com.haskov.NodeBenchMaster;
import com.haskov.QueryGenerator;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import org.junit.Test;

public class TestMergeJoinJson {
    private final static String expectedNodeType = "MergeJoin";
    private final static String filePath = "testplans/mergejoin.json";

    public void test(long size, int queryCount) {
        String argArray = "-h localhost -j " + filePath + " -S " + size + " -q " + queryCount;
        Configuration conf = Cmd.args(argArray.split(" "));
        V2.init(conf);
        NodeBenchMaster master = new NodeBenchMaster(conf);
        master.start();
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

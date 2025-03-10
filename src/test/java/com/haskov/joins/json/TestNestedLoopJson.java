package com.haskov.joins.json;

import com.haskov.Cmd;
import com.haskov.NodeBenchMaster;
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
        NodeBenchMaster master = new NodeBenchMaster(conf);
        master.start();
        V2.closeConnection();
    }

    @Test
    public void testNestedLoop() {
        test(500, 500);
        test(800, 500);
        test(1000, 100);
        test(2000, 50);
        //test(100000, 100);
    }
}

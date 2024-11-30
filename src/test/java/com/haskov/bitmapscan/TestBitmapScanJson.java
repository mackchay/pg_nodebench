package com.haskov.bitmapscan;

import com.haskov.Cmd;
import com.haskov.QueryGenerator;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.test.TestUtils;
import org.junit.Test;

import java.util.List;

public class TestBitmapScanJson {
    private final static String expectedNodeType = "BitmapIndexScan";
    private final static String expectedNodeType2 = "BitmapHeapScan";
    private final static String filePath = "testplans/bitmapscan.json";

    public void test(long size, int queryCount) {
        String argArray = "-h localhost -j " + filePath + " -S " + size + " -q " + queryCount;
        Configuration conf = Cmd.args(argArray.split(" "));
        V2.init(conf);
        QueryGenerator qb = new QueryGenerator();
        List<String> queries = qb.generate(conf.tableSize, conf.plan, conf.queryCount);
        TestUtils.testQueriesOnNode(queries.toArray(new String[0]), expectedNodeType);
        TestUtils.testQueriesOnNode(queries.toArray(new String[0]), expectedNodeType2);
    }

    @Test
    public void testBitmapScan() {
        test(1000, 1000);
        test(10000, 500);
        test(100000, 100);
    }
}

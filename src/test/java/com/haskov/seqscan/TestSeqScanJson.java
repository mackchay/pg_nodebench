package com.haskov.seqscan;

import com.haskov.Cmd;
import com.haskov.PlanAnalyzer;
import com.haskov.QueryGenerator;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.test.TestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestSeqScanJson {
    private final static String expectedNodeType = "SeqScan";
    private final static String filePath = "testplans/seqscan.json";

    public void test(long size, int queryCount) {
        String argArray = "-h localhost -j " + filePath + " -S " + size + " -q " + queryCount;
        Configuration conf = Cmd.args(argArray.split(" "));
        V2.init(conf);
        QueryGenerator qb = new QueryGenerator();
        List<String> queries = qb.generate(conf.tableSize, conf.plan, conf.queryCount);
    }

    @Test
    public void testSeqScan() {
        test(1000, 1000);
        test(10000, 500);
        test(100000, 100);
    }
}

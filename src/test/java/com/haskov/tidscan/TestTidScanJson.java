package com.haskov.tidscan;

import com.haskov.Cmd;
import com.haskov.NodeBenchMaster;
import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import org.junit.Test;

public class TestTidScanJson {
    private final static String filePath = "testplans/tid_scan.json";

    public void test(long size, int queryCount) {
        String argArray = "-h localhost -j " + filePath + " -S " + size + " -q " + queryCount;
        Configuration conf = Cmd.args(argArray.split(" "));
        V2.init(conf);
        NodeBenchMaster master = new NodeBenchMaster(conf);
        master.start();
        V2.closeConnection();
    }

    @Test
    public void testSeqScan() {
        test(1000, 1000);
        test(10000, 500);
        test(100000, 100);
    }
}

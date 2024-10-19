package com.haskov;

import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import com.haskov.test.TestUtils;

public class Main {
    public static void main(String[] args) {
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        TestUtils.testQueriesOnNode(new String[]{"select 1"}, "Result");
    }
}

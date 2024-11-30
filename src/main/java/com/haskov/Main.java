package com.haskov;

import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {
    public static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Configuration conf = Cmd.args(args);
        V2.init(conf);
        QueryGenerator g = new QueryGenerator();
        g.generate(conf.tableSize, conf.plan, conf.queryCount);
    }
}

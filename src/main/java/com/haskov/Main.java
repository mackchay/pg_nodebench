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
        PlanAnalyzer analyzer = new PlanAnalyzer(conf.sizeOfTable, conf.plan);
        for (int i = 0; i < 1000; i++) {
            String query = analyzer.buildQuery();
            System.out.println(query);
        }
    }
}

package com.haskov;

import com.haskov.bench.V2;
import com.haskov.bench.v2.Configuration;

public class Main {
    public static void main(String[] args) {
        Configuration conf = Cmd.args(args);
        V2.init(conf);

    }
}

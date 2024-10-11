package com.haskov;

import bench.v2.Configuration;
import bench.v2.Database;
import bench.v2.strategy.Strategies;
import org.apache.commons.cli.*;

import java.util.concurrent.atomic.AtomicLong;


public class Cmd {

    public static Configuration params;
    private static final String DEFPGPORT = "5432";
    private static final String DEFTIMEOUT = "10";
    private static final String DEFWORKERS = "5";
    private static final String DEFCONCURRENCY = "10";
    private static final String DEFVOLUME = "10";
    private static final String DEFRUNTYPE = Configuration.Phase.EXECUTE.toString();
    private static final String DEFSTRATEGY = "none";

    public static Configuration args(String[] args) {
        Options opt = new Options();

        /* Database options */
        opt.addOption(Option.builder("h").hasArg().argName("host").required()
                .desc("database host name").build());


        opt.addOption(Option.builder("p").hasArg().argName("port")
                .desc("database port. Defaults to " + DEFPGPORT).build());

        opt.addOption(Option.builder("d").hasArg().argName("database")
                .desc("database name. Defaults to 'postgres'").build());

        opt.addOption(Option.builder("U").hasArg().argName("username")
                .desc("user name. Defaults to 'postgres'").build());

        opt.addOption(Option.builder("P").hasArg().argName("password")
                .desc("user password").build());

        /* Workload options */
        opt.addOption(Option.builder("w").hasArg().argName("workers")
                .desc("amount of workers. Defaults to " + DEFWORKERS)
                .build());
        opt.addOption(Option.builder("c").hasArg().argName("concurrency")
                .desc("amount of concurrent workers. Defaults to " + DEFCONCURRENCY)
                .build());
        opt.addOption(Option.builder("o").hasArg().argName("run type")
                .desc("Run type (generate,run). Defaults to " + DEFRUNTYPE)
                .build());
        opt.addOption(Option.builder("v").hasArg().argName("volume")
                .desc("Volume size. Defaults to " + DEFVOLUME)
                .build());

        opt.addOption(Option.builder("t").hasArg().argName("timeout")
                .desc("test duration. Default to " + DEFTIMEOUT)
                .build());
        opt.addOption(Option.builder("s").hasArg().argName("timeout")
                .desc("Worker distribute strategy. Default to " + DEFSTRATEGY)
                .build());
        opt.addOption(Option.builder("T").hasArg().argName("txLimit")
                .desc("max amount of transactions. Disabled by default")
                .build());
        //must be over 30
        opt.addOption(Option.builder("l").hasArg().argName("cnTimeLimit")
                .desc("max life time of connection in seconds. Disabled by default")
                .build());

        opt.addOption(Option.builder("n").hasArg().argName("node").
                desc("node type in query").build());
        opt.addOption((Option.builder("C").hasArg().
                argName("Cardinality").desc("Cardinality for table data").build()));
        opt.addOption(Option.builder("S").hasArg().argName("Selectivity").
                desc("Selectivity of query").build());

        params = new Configuration();
        try {
            CommandLine cmd = new DefaultParser().parse(opt, args);

            params.workers = Integer.parseInt(cmd.getOptionValue("w", DEFWORKERS));
            params.concurrency = Integer.parseInt(cmd.getOptionValue("c", DEFCONCURRENCY));
            params.strategy = Strategies.StrategyName.valueOf(cmd.getOptionValue("s", DEFSTRATEGY).toUpperCase());
            params.volume = Integer.parseInt(cmd.getOptionValue("v", DEFVOLUME));
            params.runType = Configuration.Phase.valueOf(cmd.getOptionValue("o", DEFRUNTYPE));

            params.timeout = Integer.parseInt(cmd.getOptionValue("t", DEFTIMEOUT));
            params.txlimit = new AtomicLong(Long.parseLong(cmd.getOptionValue("T", "-1")));
            params.host = cmd.getOptionValue("h");
            params.port = Integer.parseInt(cmd.getOptionValue("p",DEFPGPORT));
            params.database = cmd.getOptionValue("d","postgres");
            params.user = cmd.getOptionValue("U","postgres");
            params.password = cmd.getOptionValue("P","postgres");
            params.timeLimit = Long.parseLong(cmd.getOptionValue("l", "0")) * 1000L;

            params.node = cmd.getOptionValue("n");
            params.selectivity = Integer.parseInt(cmd.getOptionValue("S"));
            params.cardinality = Long.parseLong(cmd.getOptionValue("C"));


        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -Xmx256m -jar pg_selectonly.jar", opt, true);
            System.out.println();
            /* Print exception at end */
            System.out.println("\033[0;1m[ERROR]: " + e.getMessage() + "\033[0m");
            System.exit(-1);
        }
        return params;
    }
}

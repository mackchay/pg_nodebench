package com.haskov;

import com.haskov.bench.v2.Configuration;
import com.haskov.bench.v2.strategy.Strategies;
import org.apache.commons.cli.*;

import java.util.concurrent.atomic.AtomicLong;

import static com.haskov.json.JsonOperations.getJsonPlan;


public class Cmd {

    public static Configuration params;
    private static final String DEF_PGPORT = "5432";
    private static final String DEF_TIMEOUT = "10";
    private static final String DEF_WORKERS = "5";
    private static final String DEF_CONCURRENCY = "10";
    private static final String DEF_VOLUME = "10";
    private static final String DEF_RUNTYPE = Configuration.Phase.EXECUTE.toString();
    private static final String DEF_STRATEGY = "none";
    private static final String DEF_TABLE_SIZE = "1000";
    private static final String DEF_QUERY_COUNT = "500";

    public static Configuration args(String[] args) {
        Options opt = new Options();

        /* Database options */
        opt.addOption(Option.builder("h").hasArg().argName("host").required()
                .desc("database host name").build());


        opt.addOption(Option.builder("p").hasArg().argName("port")
                .desc("database port. Defaults to " + DEF_PGPORT).build());

        opt.addOption(Option.builder("d").hasArg().argName("database")
                .desc("database name. Defaults to 'postgres'").build());

        opt.addOption(Option.builder("U").hasArg().argName("username")
                .desc("user name. Defaults to 'postgres'").build());

        opt.addOption(Option.builder("P").hasArg().argName("password")
                .desc("user password").build());

        /* Workload options */
        opt.addOption(Option.builder("w").hasArg().argName("workers")
                .desc("amount of workers. Defaults to " + DEF_WORKERS)
                .build());
        opt.addOption(Option.builder("c").hasArg().argName("concurrency")
                .desc("amount of concurrent workers. Defaults to " + DEF_CONCURRENCY)
                .build());
        opt.addOption(Option.builder("o").hasArg().argName("run type")
                .desc("Run type (generate,run). Defaults to " + DEF_RUNTYPE)
                .build());
        opt.addOption(Option.builder("v").hasArg().argName("volume")
                .desc("Volume size. Defaults to " + DEF_VOLUME)
                .build());

        opt.addOption(Option.builder("t").hasArg().argName("timeout")
                .desc("test duration. Default to " + DEF_TIMEOUT)
                .build());
        opt.addOption(Option.builder("s").hasArg().argName("timeout")
                .desc("Worker distribute strategy. Default to " + DEF_STRATEGY)
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
        opt.addOption(Option.builder("S").hasArg().argName("tableSize").
                desc("Size of tables. Default 1000").build());
        opt.addOption(Option.builder("j").hasArg().argName("jsonPlan").
                desc("query plan in json format").build());
        opt.addOption(Option.builder("q").hasArg().argName("query count").
                desc("count of generated queries").build());

        params = new Configuration();
        try {
            CommandLine cmd = new DefaultParser().parse(opt, args);

            params.workers = Integer.parseInt(cmd.getOptionValue("w", DEF_WORKERS));
            params.concurrency = Integer.parseInt(cmd.getOptionValue("c", DEF_CONCURRENCY));
            params.strategy = Strategies.StrategyName.valueOf(cmd.getOptionValue("s", DEF_STRATEGY).toUpperCase());
            params.volume = Integer.parseInt(cmd.getOptionValue("v", DEF_VOLUME));
            params.runType = Configuration.Phase.valueOf(cmd.getOptionValue("o", DEF_RUNTYPE));

            params.timeout = Integer.parseInt(cmd.getOptionValue("t", DEF_TIMEOUT));
            params.txlimit = new AtomicLong(Long.parseLong(cmd.getOptionValue("T", "-1")));
            params.host = cmd.getOptionValue("h");
            params.port = Integer.parseInt(cmd.getOptionValue("p", DEF_PGPORT));
            params.database = cmd.getOptionValue("d","postgres");
            params.user = cmd.getOptionValue("U","postgres");
            params.password = cmd.getOptionValue("P","postgres");
            params.timeLimit = Long.parseLong(cmd.getOptionValue("l", "0")) * 1000L;

            //params.node = cmd.getOptionValue("n");
            params.tableSize = Long.parseLong(cmd.getOptionValue("S", DEF_TABLE_SIZE));
            params.plan = getJsonPlan(cmd.getOptionValue("j"));
            params.queryCount = Integer.parseInt(cmd.getOptionValue("q", DEF_QUERY_COUNT));


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

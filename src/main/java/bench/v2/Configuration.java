package bench.v2;

import bench.v2.strategy.Strategies.StrategyName;

import java.util.concurrent.atomic.AtomicLong;

public class Configuration {
	
	public enum Phase {
		GENERATE,
		EXECUTE
	}

	/* Worker parameters */
	public Integer startPoint;
	public Integer workers;
	public Integer concurrency;
	public Integer loops;
	public Integer timeout;
	public AtomicLong txlimit;
	public String host;
	public Integer port;
	public String database;
	public String user;
	public String password;
	public Long timeLimit;
	public String node;
	public Integer selectivity;
	public Long cardinality;

	public Integer volume;
	public Phase runType;
	public StrategyName strategy;
}

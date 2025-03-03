package com.haskov.bench.v2;

import com.haskov.bench.v2.strategy.Strategies;
import com.haskov.json.JsonPlan;

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
	public Strategies.StrategyName strategy;

	public Long tableSize;
	public JsonPlan plan;
	public Integer queryCount;
}

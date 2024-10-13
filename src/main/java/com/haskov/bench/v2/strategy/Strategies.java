package com.haskov.bench.v2.strategy;

import java.util.HashMap;
import java.util.Map;

public class Strategies {

	public enum StrategyName {
		NONE,
		SEQUENT,
		RANDOM ,
		PINNING,
		SDMWISE;
	}
	
	public final Map<StrategyName, IDistributionStrategy> values = new HashMap<>();
	
	public Strategies() {
		values.put(StrategyName.NONE, new NoneStrategy());
		values.put(StrategyName.SEQUENT, new SequentStrategy());
		values.put(StrategyName.RANDOM, new RandomStrategy());
		values.put(StrategyName.PINNING, new PinningStrategy());
		values.put(StrategyName.SDMWISE, new ShardmanWiseStrategy());
	}
	
	public IDistributionStrategy getStrategy(StrategyName name) {
		return values.get(name);
	}
}

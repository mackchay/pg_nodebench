package com.haskov.bench.v2.strategy;

import com.haskov.bench.v2.DataContext;
import com.haskov.bench.v2.Database;
import com.haskov.bench.v2.strategy.Strategies.StrategyName;

public class PinningStrategy implements IDistributionStrategy {
	
	public ThreadLocal<Integer> workerConn = new ThreadLocal<Integer>();
	public IDistributionStrategy sq;
	
	@Override
	public void init(Database db) {
		sq = db.dataSourceStrategies.getStrategy(StrategyName.SEQUENT);
		sq.init(db);
	}

	@Override
	public Integer getDataSourceID(DataContext ctx) {
		if(workerConn.get() == null)
		{
			workerConn.set(sq.getDataSourceID(ctx));
		}
		return workerConn.get();	
	}

}

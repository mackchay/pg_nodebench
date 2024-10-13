package com.haskov.bench.v2.strategy;

import com.haskov.bench.v2.DataContext;
import com.haskov.bench.v2.Database;

public interface IDistributionStrategy {

	void init(Database db);
	Integer getDataSourceID(DataContext ctx);
	
}

package com.haskov.bench.v2.strategy;

import com.haskov.bench.v2.DataContext;
import com.haskov.bench.v2.Database;

public class SequentStrategy implements IDistributionStrategy {

	/* FIXME: initialize value of countDs */
	public int countDs; 
	public int nextDs;

	@Override
	public Integer getDataSourceID(DataContext ctx) {
		nextDs = (nextDs + 1) % countDs;
		return nextDs;
	}

	@Override
	public void init(Database db) {
		countDs = db.ds.size();
	}

}

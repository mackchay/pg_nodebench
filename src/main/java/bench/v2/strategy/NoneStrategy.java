package bench.v2.strategy;

import bench.v2.DataContext;
import bench.v2.Database;

public class NoneStrategy implements IDistributionStrategy {

	@Override
	public void init(Database db) {
		
	}

	@Override
	public Integer getDataSourceID(DataContext ctx) {
		return 0;
	}

}

package bench.v2.strategy;

import bench.v2.DataContext;
import bench.v2.Database;

public interface IDistributionStrategy {

	void init(Database db);
	Integer getDataSourceID(DataContext ctx);
	
}

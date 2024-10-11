package bench.v2.strategy;

import bench.v2.DataContext;
import bench.v2.Database;

import java.util.Random;

public class RandomStrategy implements IDistributionStrategy {

	public Random r;
	public int countDs;
	
	@Override
	public void init(Database db) {
		r = new Random();
		countDs = db.ds.size(); 
	}

	@Override
	public Integer getDataSourceID(DataContext ctx) {
		return r.nextInt(countDs);
	}

}

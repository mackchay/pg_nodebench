package bench.v2.strategy;

import bench.v2.DataContext;
import bench.v2.Database;
import bench.v2.strategy.Strategies.StrategyName;
import bench.v2.strategy.shardman.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ShardmanWiseStrategy implements IDistributionStrategy {

	public static final Logger log = LoggerFactory.getLogger(ShardmanWiseStrategy.class);
	public final static String QUERY_PARTS = "SELECT p.rel::oid::regclass::text as table_name,\n"
			+ "       array_agg(pnum) filter(WHERE r.srvid IS NULL) as parts,\n"
			+ "       max(pnum) + 1 AS max_pnum\n"
			+ "FROM shardman.parts p,\n"
			+ "     shardman.repgroups r\n"
			+ "WHERE p.rgid = r.id\n"
			+ "GROUP BY p.rel";

	public final Map<String, Integer[]> dict = new HashMap<>();
	public IDistributionStrategy noneStrategy;
	
	@Override
	public Integer getDataSourceID(DataContext ctx) {
		if (ctx == null) {
			return noneStrategy.getDataSourceID(ctx);
		}
		
		if (!dict.containsKey(ctx.tableName)) {
			throw new RuntimeException("Table " + ctx.tableName + " not found in partitions, but declared as context");
		}

		long rowHash = 0l;

		for (Object o : ctx.keyValues) {
			long hash;
			if (o instanceof Integer) {
				hash = Hash.hashInt((Integer) o, Hash.PartitionSeed);
			} else if (o instanceof Short) {
				hash = Hash.hashInt2((Short) o, Hash.PartitionSeed);
			} else if (o instanceof Long) {
				hash = Hash.hashInt8((Long) o, Hash.PartitionSeed);
			} else if (o instanceof String) {
				hash = Hash.hashBytes(o.toString().getBytes(), Hash.PartitionSeed);
			} else if (o instanceof UUID) {
				hash = Hash.hashUuid((UUID) o, Hash.PartitionSeed);
			} else
				throw new RuntimeException("Unknown key type:" + o.getClass().getName());
			rowHash = Hash.hashCombine64(rowHash, hash);
		}

		Integer[] partsToDS = dict.get(ctx.tableName);
		int partID = (int) Long.remainderUnsigned(rowHash, partsToDS.length);
		int ds = partsToDS[partID];
		//log.info("Found DS#{} for Part#{} by key#{}",ds, partID, ctx.keyValues.get(0));
		return ds;
	}

	@Override
	public void init(Database db) {
		noneStrategy = db.dataSourceStrategies.getStrategy(StrategyName.NONE);
		
		for (int i = 0; i < db.ds.size(); i++) {
			//log.info("DS#{}", i);
			DataSource ds = db.ds.get(i);
			try (Connection conn = ds.getConnection(); Statement st = conn.createStatement();) {

				ResultSet rs = st.executeQuery(QUERY_PARTS);
				while(rs.next()) {
					
					String tableName = rs.getString(1);
					Integer cnt = rs.getInt(3);
	
					if (!dict.containsKey(tableName)) {
						dict.put(tableName, new Integer[cnt]);
					}
	
					Integer[] sourceIDs = dict.get(tableName);
	
					try (ResultSet x = rs.getArray(2).getResultSet()) {
						while (x.next()) {
							sourceIDs[x.getInt(2)] = i;
							log.info("Table {} - {}", tableName, x.getInt(2));
						}
					}
				}
				rs.close();
			} catch (SQLException e) {
				log.error("Some error", e);
				throw new RuntimeException(e);
			}
		}
		
		boolean broken = false;
		for (Map.Entry<String, Integer[]> e : dict.entrySet()) {
			//log.info("Table name: {}", e.getKey());
			for (int i = 0; i < e.getValue().length; i++) {
				//log.info("[{}] -> {}", i, e.getValue()[i]);
				if (e.getValue()[i] == null) {
					broken = true;
				}
			}
		}
		if (broken) {
			throw new RuntimeException("Failed to initialize SDM-wise strategy");
		}
	}
	
}

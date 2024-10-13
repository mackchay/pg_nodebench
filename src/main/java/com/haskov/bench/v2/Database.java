package com.haskov.bench.v2;

import com.haskov.bench.V2;
import com.haskov.bench.v2.strategy.IDistributionStrategy;
import com.haskov.bench.v2.strategy.Strategies;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class Database {
	private static final Logger log = LoggerFactory.getLogger(Database.class);
	
	public static Boolean pooling = true;
	
	private ThreadLocal<Connection> conn = new ThreadLocal<>();
	
	public List<DataSource> ds;
	public String[] hosts; 
	
	public Strategies dataSourceStrategies;
	public IDistributionStrategy currentStrategy;
	
	public Integer NextDsNum;
	
	public String host;
	public int port;
	public String dbName;
	public String userName;
	public String passwd;
	public int poolSize;
	public long maxLifetime;
	
	public Database(String host, int port, String dbName, String userName, String passwd, int poolSize, Long maxLifetime) {
		this.host = host;
		this.port = port;
		this.dbName = dbName;
		this.userName = userName;
		this.passwd = passwd;
		
		this.maxLifetime = maxLifetime;
		this.poolSize = poolSize;
		this.dataSourceStrategies = new Strategies();
	}
	
	public DataSource getDataSource(DataContext ctx) {
		if (ds == null) {
			synchronized (this) {
				if (ds == null) {
					List<DataSource> x = new ArrayList<DataSource>();
					NextDsNum = 0;
					if (pooling) {
						hosts = host.split(",");
						for ( String h: hosts)
						{
							HikariConfig config = new HikariConfig();
							config.setJdbcUrl("jdbc:postgresql://" + h + ":" + Integer.toString(port) + "/" + dbName + "?prepareThreshold=1&binaryTransfer=false");
							config.setUsername(userName);
							config.setPassword(passwd);
							config.setAutoCommit(V2.autoCommit);
							config.addDataSourceProperty("cachePrepStmts", "true");
							config.addDataSourceProperty("prepStmtCacheSize", "250");
							config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
							config.setInitializationFailTimeout(1);
							config.setConnectionTimeout(300000);
							config.setConnectionInitSql("set search_path=dev,public");
							config.setMaximumPoolSize(poolSize);
							config.setMaxLifetime(maxLifetime);
							x.add(new HikariDataSource(config));
						}
						
					} else {
						String[] hosts = host.split(",");
						for ( String h: hosts)
						{
							PGSimpleDataSource pgds = new PGSimpleDataSource();
							pgds.setServerNames(new String[] {h});
							pgds.setDatabaseName(dbName);
							pgds.setUser(userName);
							pgds.setPassword(passwd);
							pgds.setPortNumbers(new int[] {port});
							pgds.setConnectTimeout(1000);
							x.add(pgds);
						}
					}
					
					ds = x;
				}
			}
		}
		
		if (currentStrategy == null) {
			synchronized (this) {
				if (currentStrategy == null) {
					IDistributionStrategy x = dataSourceStrategies.getStrategy(V2.params.strategy);
					x.init(this);
					currentStrategy = x;
				}
			}
		}
		
		return ds.get(currentStrategy.getDataSourceID(ctx));
	}
	
	public DataSource getDataSource() {
		return getDataSource(null);
	}
	
	public interface CallableStatement<V> {
	    /**
	     * Computes a result, or throws an exception if unable to do so.
	     *
	     * @return computed result
	     * @throws Exception if unable to compute a result
	     */
	    V call(Connection conn) throws Exception;
	}
	
	public void push(Connection conn) {
		this.conn.set(conn);
	}
	
	public void pop() {
		this.conn.set(null);
	}
	
	public Connection get() {
		return this.conn.get();
	}
	
	public <V> V execute(CallableStatement<V> c) {
		return execute(c, null);
	}
	
	public <V> V execute(CallableStatement<V> c, Callable<Void> handlerOnError) {
		try
		{
			if (conn.get() == null) {
				try (Connection localConn = getDataSource().getConnection()) {
					return c.call(localConn);
				}
			} else {
				return c.call(conn.get());
			}
		} catch (SQLException e) {
			
			log.info("{}", e.getMessage());
			if(e.getMessage().contains("could not serialize access due to concurrent update"))
				return null;
			if (handlerOnError != null)
				try{
					handlerOnError.call();
					return null;
				} catch (Exception x) {
					throw new RuntimeException("Exception occured during error handling...", x);
				}
			else {
				throw new RuntimeException("SQL exception occured during DB...", e);
			}
		} catch (Exception e) {
			log.info("------------exception ...{}-----------------", e.getMessage());
			//log.error("Unknown exception occured during connecting...", e);s
			throw new RuntimeException("Exception occured during DB...", e);
		}
		
	}
	
	
	public interface CallableResultSet<V> {
	    /**
	     * Computes a result, or throws an exception if unable to do so.
	     *
	     * @return computed result
	     * @throws Exception if unable to compute a result
	     */
	    V call(ResultSet rs) throws Exception;
	}
	
	public interface VoidResultSet {
	    /**
	     * Computes a result, or throws an exception if unable to do so.
	     *
	     * @return computed result
	     * @throws Exception if unable to compute a result
	     */
	    void call(ResultSet rs) throws Exception;
	}
	
	
	public boolean insert(String sql, Object...binds) {
		return execute((conn) -> {
			try(PreparedStatement pstmt = conn.prepareStatement(sql);) {
				for(int i = 0; i < binds.length; i++) {
					pstmt.setObject(i + 1, binds[i]);
				}
				
				return pstmt.execute();
			}
		});
	}
	
	public <V> List<V> list(CallableResultSet<V> rsHandler, String sql, Object... binds) {
		return execute((conn) -> {
			List<V> res = new ArrayList<V>();
			try(PreparedStatement pstmt = conn.prepareStatement(sql);) {

				for(int i = 0; i < binds.length; i++) {
					pstmt.setObject(i + 1, binds[i]);
				}
				
				try (ResultSet rs = pstmt.executeQuery()) {
					while (rs.next()) {
						V e = rsHandler.call(rs);
						if (e != null)
							res.add(e);
					}
				}
			}
			return res;
		});
	}
	
	public boolean selectSingle(VoidResultSet rsHandler, String sql, Object... binds) {
		return execute((conn) -> {
			try(PreparedStatement pstmt = conn.prepareStatement(sql);) {
				for(int i = 0; i < binds.length; i++) {
					pstmt.setObject(i + 1, binds[i]);
				}
				
				try (ResultSet rs = pstmt.executeQuery()) {
					if (rs.next()) {
						rsHandler.call(rs);
						return true;
					}
				}
			}
			return false;
		});
	}
	
	public interface CallableMapResultSet<K, V> {
	    /**
	     * Computes a result, or throws an exception if unable to do so.
	     *
	     * @return computed result
	     * @throws Exception if unable to compute a result
	     */
	    void call(ResultSet rs, Map<K,V> map) throws Exception;
	}
	
	public <K, V> Map<K, V> map(CallableMapResultSet<K, V> rsHandler, String sql, Object... binds) {
		return execute((conn) -> {
			Map<K, V> map = new HashMap<>();
			try(PreparedStatement pstmt = conn.prepareStatement(sql);) {

				for(int i = 0; i < binds.length; i++) {
					pstmt.setObject(i + 1, binds[i]);
				}
				
				try (ResultSet rs = pstmt.executeQuery()) {
					while (rs.next()) {
						rsHandler.call(rs, map);
					}
				}
			}
			return map;
		});
	}

	public void close() throws SQLException {
		// Close connections from ThreadLocal
		Connection localConn = conn.get();
		if (localConn != null) {
			try {
				if (!localConn.isClosed()) {
					localConn.close();
					log.info("ThreadLocal connection closed.");
				}
			} catch (SQLException e) {
				log.error("Error closing ThreadLocal connection: {}", e.getMessage());
			} finally {
				conn.remove();
			}
		}

		// Close DataSources
		if (ds != null) {
			for (DataSource d : ds) {
				if (d instanceof HikariDataSource) {
                    ((HikariDataSource) d).close();
                    log.info("HikariDataSource closed.");
                }
			}
			ds = null;
			log.info("All database connections closed.");
		}
	}
}

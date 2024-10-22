package com.haskov.bench;

import com.haskov.bench.v2.Configuration.Phase;
import com.haskov.bench.v2.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class V2 {
	
	static {
		System.setProperty("logback.configurationFile", "logback.xml");
		System.setProperty("org.slf4j.simpleLogger.showDateTime","true");
		System.setProperty("org.slf4j.simpleLogger.dateTimeFormat","yyyy-MM-dd HH:mm:ss:SSS Z");
	}
	
	private final static String lineSep = "-------------------------------------------------------------------";
	
	public static final Logger log = LoggerFactory.getLogger(V2.class);

	private static ScheduledExecutorService pool;
	
	public enum RangeOption{
		RANDOM,
		SHARED
	}
	
	public static Database db;
	public static Configuration params;
	
	public static Boolean verbosity = false;
	public static Boolean sessionAffinity = true;
	public static Boolean autoCommit = true;
	
	public static AtomicBoolean dbGen = new AtomicBoolean(false);

	public static void sql(String sql, Object... binds) {
		db.<Boolean>execute((conn) -> {
			try(PreparedStatement pstmt = conn.prepareStatement(sql);) {

				for(int i = 0; i < binds.length; i++) {
					pstmt.setObject(i + 1, binds[i]);
				}

				return pstmt.execute();
			}
		});
	}
	
	public static void sqlNoPrepare(String sql) {
		db.<Boolean>execute((conn) -> {
			try(Statement stmt = conn.createStatement();)
			{
				return stmt.execute(sql);
			}
		});
	}
	
	public static <V> V sqlCustom(Database.CallableStatement<V> custom) {
		return db.execute(custom);
	}
	
	@SuppressWarnings("unchecked")
	public static <E> E selectOne(String sql, Object... binds) {
		List<E> x = new ArrayList<>();
		db.selectSingle((rs) -> {
			x.add((E) rs.getObject(1));
		}, sql, binds);
		return x.isEmpty() ? null : x.get(0);
	}
	
	@SuppressWarnings("unchecked")
	public static <E> List<E> selectColumn(String sql, Object... binds) {
		return db.list((rs) -> {return (E) rs.getObject(1);}, sql, binds);
	}

	public static List<List<String>> select(String sql, Object... binds) {
		return db.list(rs -> {
			int columnCount = rs.getMetaData().getColumnCount();
			List<String> row = new ArrayList<>(columnCount);

			for (int i = 1; i <= columnCount; i++) {
				row.add(rs.getObject(i).toString());
			}

			return row;
		}, sql, binds);
	}
	
	public static List<String> explainResults(String sql, Object... binds) {
		return selectColumn("explain (analyze, verbose, buffers, costs off) " + sql, binds);
	}

	public static void explain(Logger log, String sql, Object... binds) {
		List<String> lines = selectColumn("explain (analyze, verbose, buffers) " + sql, binds);
		if (log != null)
			log.info("Actual plan \n{}\n{}\n{}", lineSep, String.join("\n", lines), lineSep);
	}
	
	private static void preInit() {
		Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
			log.error("Test stopped due to error:", unwrap(e));
		});
	}

	public static void init(Configuration conf) {
		preInit();
		params = conf;

		db = new Database(params.host,
					params.port,
					params.database,
					params.user,
					params.password,
					params.workers,
					params.timeLimit
					);
	}
	
	public static void begin() {
		try{
			db.get().setAutoCommit(false);
		} catch (SQLException e) {
			throw new RuntimeException("Error on setAutoCommit(false)", e);
		}
	}

	public static void commit() {
		try {
			db.get().commit();
		} catch (SQLException e) {
			throw new RuntimeException("Error on commit", e);
		}
	}
	
	public static void ctx(String tableName, List<Object> keyValues, WorkerState st, final WorkerUnit x) {
		if (db.get() != null) {
			throw new RuntimeException("Connection is already defined");
		}
		try (Connection cc = db.getDataSource(new DataContext(tableName, keyValues)).getConnection()){
			db.push(cc);
			x.iterate(st);
			db.pop();
		} catch (SQLException e) {
			throw new RuntimeException("Error on commit", e);
		}
	}
	
	public static void transaction(String tableName, List<Object> keyValues, WorkerState st, final WorkerUnit x) {
		try {
			Connection c = db.get();
			if (c != null) {
				c.setAutoCommit(false);
				x.iterate(st);
				c.commit();
			} else {
				try (Connection cc = db.getDataSource(new DataContext(tableName, keyValues)).getConnection()){
					db.push(cc);
					cc.setAutoCommit(false);
					x.iterate(st);
					cc.commit();
					db.pop();
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error on commit", e);
		}
	}
	
	public static void transaction(WorkerState st, final WorkerUnit x) {
		try {
			Connection c = db.get();
			if (c != null) {
				c.setAutoCommit(false);
				x.iterate(st);
				c.commit();
			} else {
				try (Connection cc = db.getDataSource().getConnection()){
					db.push(cc);
					cc.setAutoCommit(false);
					x.iterate(st);
					cc.commit();
					db.pop();
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error on commit", e);
		}
	}
	
	public static Results parallel(final WorkerUnit x) {
		List<Snap> metrics = new ArrayList<>(1000);
		int period = 1000;
		long durNs;
		String logResultsIntro;
		
		if ((params.runType.compareTo(Phase.GENERATE) >= 0) && dbGen.get()) {
			log.info("Starting {} workers for generate {} rows", params.workers, params.volume);
			durNs = parallelInternal(x, params.volume, Integer.MAX_VALUE, metrics, period, false);
			logResultsIntro = "Generation completed after";
		}
		else if (params.runType.compareTo(Phase.EXECUTE) >= 0) {
			log.info("Starting {} workers for {} seconds", params.workers, params.timeout);
			durNs = parallelInternal(x, 0, params.timeout, metrics, period, verbosity);
			logResultsIntro = "Test completed after";
		}
		else
			return null;
		
		if (metrics.size() > 0) {
			Results r = new Results(metrics, period, durNs);
			r.logSummary(log, logResultsIntro);
			return r;
		} else return null;
	}
	
	private static long parallelInternal(final WorkerUnit x, long iterationLimit, int timeout, List<Snap> snaps, int period, boolean monVerbose) {
		pool = Executors.newScheduledThreadPool(params.workers);
		ScheduledExecutorService mon = Executors.newScheduledThreadPool(1);

		CyclicBarrier c = new CyclicBarrier(params.workers + 1);

		Integer mainLimit = 0;
		Integer extraUnits = 0;
		
		if (iterationLimit > 0) {
			mainLimit = params.volume / params.workers;
			extraUnits = params.volume % params.workers;
		}
		
		db.getDataSource();
		
		List<WorkerState> states = new ArrayList<WorkerState>();
		for (int i = 0; i < params.workers; i++) {
			WorkerState st = new WorkerState();	
			
			st.iterationsDone = 0;
			st.startPoint = c;
			
			st.iterationLimit = mainLimit;
			st.iterationLimit += (i < extraUnits) ? 1 : 0;
			
			states.add(st);
			
			pool.schedule(() -> {
				try {
					try (Connection conn = db.getDataSource().getConnection()) {
						st.startPoint.await();
					}
					
					do {
						if (iterationLimit > 0 && st.iterationsDone >= st.iterationLimit)
							break;
						if (sessionAffinity) {
							try (Connection conn = db.getDataSource().getConnection()) {
								db.push(conn);
								x.iterate(st);
							} finally {
								db.pop();
							}
						} else {
							x.iterate(st);
						}
						
						st.iterationsDone++;
					} while (!st.stop.get());
				} catch (BrokenBarrierException e) {
					return null;
				} catch (Throwable e) {
					log.error("Occuried error", e);
					throw e;
				}
				
				return null;
			}, 0, TimeUnit.SECONDS);
		}
		
		log.info("Waiting workers' readiness");
		
		try {
			/* Let's wait for all threads to be ready */
			c.await();
		} catch (BrokenBarrierException | InterruptedException e) {
			log.error("Occuried error", e);
			return -1;
		}
		
		Long startTime = System.nanoTime();
		if (snaps != null) {
			AtomicLong curIter = new AtomicLong(0);
			
			Long start = System.nanoTime();
			Long initialDelay = 1000 - (System.currentTimeMillis() % 1000);
			mon.scheduleAtFixedRate(() -> {
				long iterations = 0;
				
				Long n = System.nanoTime();
				for (WorkerState st : states) {
					iterations += st.iterationsDone;
				}
				Long d = System.nanoTime() - n;
				Long delta = iterations - curIter.get();
				if (monVerbose)
					log.info("+{}  (took {} ns)", delta, d);
				curIter.set(iterations);
				
				Snap p = new Snap();
				p.ts = n - start;
				p.iterations = iterations;
				p.delta = delta;
				p.tookNs = d;
				
				snaps.add(p);
			}, initialDelay, period, TimeUnit.MILLISECONDS);
		}
		
		pool.shutdown();
		try {
			pool.awaitTermination(timeout, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		Long endTime = System.nanoTime();
		
		if (snaps != null) {
			mon.shutdown();
		}
		
		for (WorkerState st : states) {
			st.stop.set(true);
		}
		
		return endTime - startTime;
	}
	
	/* Variables */
	public static Var var(String sql, RangeOption... options) {
		Var res = new Var();
		for (RangeOption option : options ) {
			if (option == RangeOption.RANDOM) {
				res.rnd = new Random();
			}
			
			if (option == RangeOption.SHARED) {
				//TODO: handle shared & rename "shared" to more reasonable
				throw new UnsupportedOperationException();
			}
		}
		
		
		db.selectSingle((rs) -> {
			res.start = rs.getLong(1);
			res.end = rs.getLong(2);
			return;
		}, sql);
		
		return res;
	}
	
	public static Var var(Long min, Long max, RangeOption... options) {
		Var res = new Var();
		for (RangeOption option : options ) {
			if (option == RangeOption.RANDOM) {
				res.rnd = new Random();
			}
			
			if (option == RangeOption.SHARED) {
				//TODO: handle shared & rename "shared" to more reasonable
				throw new UnsupportedOperationException();
			}
		}
		
		res.start = min;
		res.end = max;
		
		return res;
	}
	
	public static Var var(Integer min, Integer max, RangeOption... options) {
		Var res = new Var();
		for (RangeOption option : options ) {
			if (option == RangeOption.RANDOM) {
				res.rnd = new Random();
			}
			
			if (option == RangeOption.SHARED) {
				//TODO: handle shared & rename "shared" to more reasonable
				throw new UnsupportedOperationException();
			}
		}
		
		res.start = min.longValue();
		res.end = max.longValue();
		
		return res;
	}
	
	public static Throwable unwrap(Throwable throwable) {
		Objects.requireNonNull(throwable);
		Throwable rootCause = throwable;
		while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
			rootCause = rootCause.getCause();
		}
		return rootCause;
	}
	
	private static String getDatabaseSettingValue(String gucName) {
		//TODO: fetch value from pg_settings
		return "";
	}
	
	public static void requireSettings(String gucName, Comparable<String> comparator) {
		String value = getDatabaseSettingValue(gucName);
		if (comparator.compareTo(value) == 0) {
			return;
		}
		
		//TODO: set value
	}
	
	public static void psql(String filename, Integer hostNum) {
		try (Connection conn = db.getDataSource().getConnection()) {
			log.info("Execute SQL script {} via psql host {}", filename, db.hosts[hostNum]);
			PSQL.executeFile(filename, hostNum);
			log.info("Completed SQL script");
		}
		catch (Exception e)
		{
			log.error("Occuried error", e);
		}
	}
	
	public static void requireData(String checkSQL, String filename) {
		requireData(checkSQL, filename, 0);
	}
	
	public static void requireData(String checkSQL, String filename, Integer hostNum) {
		Callable<Void> psql = () -> {
			log.info("Execute SQL script {} via psql host {}", filename, db.hosts[hostNum]);
			psql(filename, hostNum);
			log.info("Completed SQL script");
			return null;
		};
		
		requireData(checkSQL, psql);
	}
	
	public static void requireData(String checkSQL, String filename, String username, String password, String database, Integer hostNum) {
		Callable<Void> psql = () -> {
			log.info("Execute SQL script {} via psql host {} under user {} in database {}", filename, db.hosts[hostNum], username, database);
			PSQL.executeFile(filename, username, password, database, hostNum);
			log.info("Completed SQL script");
			return null;
		};
		
		requireData(checkSQL, psql);
	}
	
	public static void requireData(String checkSQL, Callable<Void> psql) {
		// check datasource
		try {
			db.getDataSource();
		} catch (Throwable e) {
			log.error("Some error", e);
			try{
				dbGen.set(true);
				log.info("Can't get DS, safe mode...");
				psql.call();
			} catch (Exception x) {
				log.error("Some error", e);
				throw new RuntimeException("Exception occured during error handling...", x);
			} finally {
				dbGen.set(false);
			}
		}
		
		Database.CallableStatement<Boolean> checkStmt = (conn) -> {
			try(PreparedStatement pstmt = conn.prepareStatement(checkSQL);) {
				return pstmt.execute();
			}
		};
		
		Callable<Void> handleOnError = () -> {
			try{
				dbGen.set(true);
				log.info("Handle SQL error, safe mode...");
				psql.call();
			} finally {
				dbGen.set(false);
			}
			
			return null;
		};
		
		db.<Boolean>execute(checkStmt, handleOnError);
	}
	
	public static void assertSimilar(Results a, Results b, String msgFormat, Object... params) {
		if (!a.similar(b)) {
			log.error("TEST FAILED: " + msgFormat, params);
		} else {
			log.info("TEST PASSED: " + msgFormat, params);
		}
	}
	
	public static void logResults(Results res) {
		log.info("Test results: last 5 sec {} tps, overall {} tps, {} iterations", res.tpsLast5sec, res.tps, res.iterations);
	}

	public static Map<String, String> getColumnsAndTypes(String tableName) {
		Map<String, String> columnsAndTypes = new HashMap<>();

		// Get metadata from table
		try (Connection conn = db.getDataSource().getConnection()) {
			DatabaseMetaData metaData = conn.getMetaData();
			try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
				while (columns.next()) {
					String columnName = columns.getString("COLUMN_NAME");
					String columnType = columns.getString("TYPE_NAME");
					columnsAndTypes.put(columnName, columnType);
				}
			}
		} catch (Exception e) {
			log.error("Some error", e);
			throw new RuntimeException("Exception occured during error handling...", e);
		}

		return columnsAndTypes;
	}

	public static void closeConnection() {
		if (db != null) {
			try {
				if (pool != null) {
					pool.shutdown();
					pool.awaitTermination(10, TimeUnit.SECONDS);
				}

				db.close();
				log.info("Database connection closed successfully.");
			} catch (SQLException e) {
				log.error("Failed to close database connection.", e);
			} catch (InterruptedException e) {
				log.error("Thread was interrupted while waiting for thread pool termination.", e);
			}
		}
	}
}

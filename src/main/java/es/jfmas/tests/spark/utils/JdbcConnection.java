package es.jfmas.tests.spark.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.SortedMap;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public final class JdbcConnection {

	private static HikariDataSource dataSource;
	private static MetricRegistry registry = new MetricRegistry();
	
	
	static {
		
		HikariConfig config = new HikariConfig();
		config.setPoolName("Spark");
		config.setDriverClassName("oracle.jdbc.driver.OracleDriver");
		config.setJdbcUrl(ConfigApp.DB_URL);
		config.setUsername(ConfigApp.DB_USER);
		config.setPassword(ConfigApp.DB_PASSWORD);
		config.setMaximumPoolSize(ConfigApp.MAX_POOL_SIZE);
		config.setConnectionTestQuery("SELECT 1 FROM DUAL");
		
		// Enable metrics
		config.setMetricRegistry(registry);
		
		dataSource = new HikariDataSource(config);
		
	}
	
	public static Connection getConnection(){
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@SuppressWarnings("all")
	public static void logActiveConnections(){
		SortedMap<String, Gauge> map = registry.getGauges();
		Gauge gauge = map.get("Spark.pool.ActiveConnections");
		Date now = new Date();
		System.out.println(now + " - Spark.pool.ActiveConnections : " + gauge.getValue());
	}
	
}

package es.jfmas.tests.spark.utils;


/**
 * App Configuration
 * @author jfmas
 *
 */
public final class ConfigApp {

	/** Application configuration **/
	public static final int NUM_NODES = 100; 
	public static final int BATCH_SIZE = 10000; 
	public static final boolean LOG_DS_STATISTICS = false;
	
	/** Connection pool configuration **/
	public static final Integer MAX_POOL_SIZE = 50;
	public static final String DB_URL = "jdbc:oracle:thin:@localhost:1521:XE";
	public static final String DB_USER = "spark";
	public static final String DB_PASSWORD = "123456";
	
	static {
		// TODO Read from properties file
	}
}

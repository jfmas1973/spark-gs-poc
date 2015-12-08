package es.jfmas.tests.spark;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import es.jfmas.tests.spark.model.FlightModel;
import es.jfmas.tests.spark.service.FlightService;
import es.jfmas.tests.spark.utils.ConfigApp;

public class MigrateGSApp implements Serializable {

	/** Serial Id **/
	private static final long serialVersionUID = 8062176647661101782L;
	
	public static void convertToJson() {
		
		//Start the job
	    SparkConf conf = new SparkConf().setAppName("MigrateGSApp").setMaster("local["+ConfigApp.NUM_NODES+"]");
	    JavaSparkContext sc = new JavaSparkContext(conf);
		
	    int partialSize = ConfigApp.BATCH_SIZE;
	    int currentPosition = 0;
	    int totalCount = 0;
	    while(partialSize == ConfigApp.BATCH_SIZE) {
		    List<FlightModel> listFlights = FlightService.readFlights(currentPosition, ConfigApp.BATCH_SIZE);
		    partialSize = listFlights.size();
		    currentPosition = currentPosition + partialSize;
		    if(partialSize == 0){
		    	continue; // end of process
		    }
		    JavaRDD<FlightModel> rddModels = sc.parallelize(listFlights, ConfigApp.NUM_NODES);
		    
		    JavaRDD<Integer> rddResults = rddModels.map(new Function<FlightModel, Integer>() {
				/** Serial id **/
				private static final long serialVersionUID = 5666614119965640487L;
				@Override
				public Integer call(FlightModel model) throws Exception {
					return FlightService.saveAsJson(model);
				}
			});
		    int partialCount = rddResults.reduce(new Function2<Integer, Integer, Integer>() {
				/** Serial id **/
				private static final long serialVersionUID = -4776600191302170825L;
				@Override
				public Integer call(Integer arg0, Integer arg1) throws Exception {
					return arg0 + arg1;
				}
			}); 
		    totalCount = totalCount + partialCount;
	    }
	    
	    System.out.println("Total rows saved as Json: " + totalCount);	    
		
	    sc.close();
		
	}
	
	
	public static void convertToJsonSqlContext() {
		
		//Start the job
	    SparkConf conf = new SparkConf().setAppName("MigrateGSApp").setMaster("local["+ConfigApp.NUM_NODES+"]");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    SQLContext sqlContext = new SQLContext(sc);
		
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", "jdbc:oracle:thin:@localhost:1521:XE");
		options.put("dbtable", "FLIGHT");
		options.put("user", "SPARK");
		options.put("password", "123456");
		
		options.put("partitionColumn", "FLIGHTID");
		options.put("lowerBound", "400770");
		options.put("upperBound", "5671598");
		options.put("numPartitions", ""+ConfigApp.NUM_NODES);

		DataFrame jdbcDF = sqlContext.read().format("jdbc").options(options).load();
		
		long total = jdbcDF.count();
		
		System.out.println("Total rows : " + total);
		
	}
	
	
	public static void importFromFileToDb() {
		
		String logFile = "/home/jfmas/Descargas/FlightsDb/Test.csv"; // Should be some file on your system
		//String logFile = "/home/jfmas/Descargas/FlightsDb/1990.csv"; // Should be some file on your system
	    SparkConf conf = new SparkConf().setAppName("MigrateGSApp").setMaster("local["+ConfigApp.NUM_NODES+"]");
		//SparkConf conf = new SparkConf().setAppName("MigrateGSApp").setMaster("spark://hp-pavilion-g6:7077");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    JavaRDD<String> logData = sc.textFile(logFile, ConfigApp.NUM_NODES).cache();
	    //JavaRDD<String> logData = sc.textFile(logFile).cache();
	    
	    System.out.println("Partitions : " + logData.partitions().size());
	    	    
	    JavaRDD<Integer> resultData = logData.map(new Function<String, Integer>() {
	    	/** Serial id **/
			private static final long serialVersionUID = 7874392453597850468L;
			@Override
	    	public Integer call(String line) throws Exception {
				if(line.startsWith("Year")){
					return 0;
				}
	    		return FlightService.saveDataFromLine(line);
	    	}
		});
	    
	    Integer result = resultData.reduce(new Function2<Integer, Integer, Integer>() {
			/** Serial id **/
			private static final long serialVersionUID = -4776600191302170825L;
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		}); 
		
		System.out.println("Total Lines saved : " + result);
				
	    sc.close();
	}
	
}

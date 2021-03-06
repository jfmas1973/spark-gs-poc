package es.jfmas.tests.spark;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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

	
	public static void convertToJsonC() {
		
		//Start the job
	    SparkConf conf = new SparkConf().setAppName("MigrateGSApp").setMaster("local["+ConfigApp.NUM_NODES+"]");
	    JavaSparkContext sc = new JavaSparkContext(conf);
		
	    int partialSize = ConfigApp.BATCH_SIZE;
	    int currentPosition = 0;

	    JavaRDD<FlightModel> rddTotalModels = sc.parallelize(new ArrayList<FlightModel>(), ConfigApp.NUM_NODES);

	    System.out.println(">>>> Build the big RDD");
	    while(partialSize == ConfigApp.BATCH_SIZE) {
		    List<FlightModel> listFlights = FlightService.readFlights(currentPosition, ConfigApp.BATCH_SIZE);
		    partialSize = listFlights.size();
		    currentPosition = currentPosition + partialSize;
		    if(partialSize == 0){
		    	continue; // end of process
		    }
		    JavaRDD<FlightModel> rddModels = sc.parallelize(listFlights, ConfigApp.NUM_NODES);
		    rddTotalModels = rddTotalModels.union(rddModels);
	    }
	    
	    System.out.println(">>>> Save the big RDD into db");
	    JavaRDD<Integer> rddResults = rddTotalModels.map(new Function<FlightModel, Integer>() {
			/** Serial id **/
			private static final long serialVersionUID = 5666614119965640487L;
			@Override
			public Integer call(FlightModel model) throws Exception {
				return FlightService.saveAsJson(model);
			}
		});

	    System.out.println(">>>> Count results");
	    int totalCount = rddResults.reduce(new Function2<Integer, Integer, Integer>() {
			/** Serial id **/
			private static final long serialVersionUID = 4828577683865983766L;
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		}); 
	    
	    System.out.println("Total rows saved as Json (version C) : " + totalCount);	    
	    
	    sc.close();
	    
	}
	
	public static void convertToJsonB() {
		//Start the job
	    SparkConf conf = new SparkConf().setAppName("MigrateGSApp").setMaster("local["+ConfigApp.NUM_NODES+"]");
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    int totalFlights = FlightService.countTotalFlights();
	    List<Integer> blocks = new ArrayList<Integer>();
	    
	    int currentPosition = 0;
	    while(currentPosition < totalFlights) {
	    	blocks.add(currentPosition);
	    	currentPosition = currentPosition + ConfigApp.BATCH_SIZE;
	    }
	    
	    JavaRDD<Integer> rddBlocks = sc.parallelize(blocks, ConfigApp.NUM_NODES);
	    JavaRDD<List<FlightModel>> rddListModels = rddBlocks.map(new Function<Integer, List<FlightModel>>() {
			/** Serial id **/
			private static final long serialVersionUID = -3198082958185451408L;
			@Override
			public List<FlightModel> call(Integer arg0) throws Exception {
				return FlightService.readFlights(arg0, ConfigApp.BATCH_SIZE);
			}
		});

	    JavaRDD<FlightModel> rddModels = rddListModels.flatMap(new FlatMapFunction<List<FlightModel>, FlightModel>() {
			/** Serial id **/
			private static final long serialVersionUID = -3198082958385451408L;
			@Override
			public Iterable<FlightModel> call(List<FlightModel> t)
					throws Exception {
				return t;
			}	    	
		});	    	    
	   	    
	    JavaRDD<Integer> rddResults = rddModels.map(new Function<FlightModel, Integer>() {
			/** Serial id **/
			private static final long serialVersionUID = 5666614119965640487L;
			@Override
			public Integer call(FlightModel model) throws Exception {
				return FlightService.saveAsJson(model);
			}
		});

	    
	    int totalCount = rddResults.reduce(new Function2<Integer, Integer, Integer>() {
			/** Serial id **/
			private static final long serialVersionUID = 4828577683865983766L;
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		}); 
	    
	    System.out.println("Total rows saved as Json (version B) : " + totalCount);	    
	    
	    sc.close();
	    
	}
	
	
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

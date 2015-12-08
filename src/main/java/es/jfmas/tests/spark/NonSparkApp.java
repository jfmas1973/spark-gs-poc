package es.jfmas.tests.spark;

import java.io.BufferedReader;
import java.io.FileReader;

import es.jfmas.tests.spark.service.FlightService;

public class NonSparkApp {

	public static final long MAX_ITERATION = 10000;
	
	public static void main(String[] args) {
		
		long initTime = System.currentTimeMillis();

		try {
			//FileReader filein = new FileReader("/home/jfmas/Descargas/FlightsDb/1990.csv");
			FileReader filein = new FileReader("/home/jfmas/Descargas/FlightsDb/Test.csv");
			BufferedReader reader = new BufferedReader(filein);
			
			long totalLines = 0;
			while(reader.ready()){
				String line = reader.readLine();
				if(line.startsWith("Year")){
					continue;
				}
				totalLines += FlightService.saveDataFromLine(line);
				
				if(totalLines % 1000 == 0){
					//JdbcConnection.logActiveConnections();
				}
				
			}
			System.out.println("Total Lines saved : " + totalLines);
			
			long elapsedTime = (System.currentTimeMillis() - initTime) / 1000; 
			System.out.println("Elapsed Time : " + elapsedTime + " sec");
			
			reader.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}

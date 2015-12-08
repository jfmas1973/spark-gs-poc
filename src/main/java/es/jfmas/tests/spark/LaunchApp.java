package es.jfmas.tests.spark;

public class LaunchApp {
	
	public static void main(String[] args) {
		
		long initTime = System.currentTimeMillis();
		
		//MigrateGSApp.importFromFileToDb();
		//MigrateGSApp.convertToJsonSqlContext();
		//MigrateGSApp.convertToJson();
		MigrateGSApp.convertToJsonB();
		
		long elapsedTime = (System.currentTimeMillis() - initTime) / 1000; 
		System.out.println("Elapsed Time : " + elapsedTime + " sec");
		
	}
	
}

package es.jfmas.tests.spark.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import es.jfmas.tests.spark.MigrateGSApp;
import es.jfmas.tests.spark.exception.RetryException;
import es.jfmas.tests.spark.model.FlightModel;
import es.jfmas.tests.spark.utils.JdbcConnection;
import es.jfmas.tests.spark.utils.MappingUtils;

public final class FlightService {

	private static final String INSERT_FLIGHT = "Insert into FLIGHT (FLIGHTID,YEAR,MONTH,DAYOFMONTH,DAYOFWEEK,DEPTIME,CRSDEPTIME,ARRTIME,"
			+ "CRSARRTIME,UNIQUECARRIER,FLIGHTNUM,TAILNUM,ACTUALELAPSEDTIME,CRSELAPSEDTIME,AIRTIME,ARRDELAY,DEPDELAY,ORIGIN,DEST,DISTANCE,"
			+ "TAXIIN,TAXIOUT,CANCELLED,CANCELLATIONCODE,DIVERTED,CARRIERDELAY,WEATHERDELAY,NASDELAY,SECURITYDELAY,LATEAIRCRAFTDELAY) "
			+ "values (flight_seq.nextval,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private static final String SELECT_FLIGHT = "select * from (select rownum as pos, F.* from (SELECT FLIGHTID,YEAR,MONTH,DAYOFMONTH,DAYOFWEEK,DEPTIME,CRSDEPTIME,ARRTIME,CRSARRTIME,"
			+ "UNIQUECARRIER,FLIGHTNUM,TAILNUM,ACTUALELAPSEDTIME,CRSELAPSEDTIME,AIRTIME,ARRDELAY,DEPDELAY,ORIGIN,DEST,DISTANCE,TAXIIN,TAXIOUT,CANCELLED,CANCELLATIONCODE,DIVERTED,"
			+ "CARRIERDELAY,WEATHERDELAY,NASDELAY,SECURITYDELAY,LATEAIRCRAFTDELAY FROM FLIGHT) F) "
			+ "where pos between ? and ?";
	
	private static final String INSERT_FLIGHT_JSON = "Insert into FLIGHT_JSON (JSONID,CONTENT) values (flight_json_seq.nextval,?)";
	
	public static List<FlightModel> readFlights(int position, int size){
		List<FlightModel> resultList = new ArrayList<FlightModel>();
		
		try(Connection conn = JdbcConnection.getConnection()){
			
			PreparedStatement pstmt = conn.prepareStatement(SELECT_FLIGHT);
			pstmt.setInt(1, position + 1);
			pstmt.setInt(2, position + size);
			
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next()){
				FlightModel model = new FlightModel();
				int step = 2;
				model.FlightId = rset.getLong(step++);
				model.Year = rset.getString(step++);
				model.Month = rset.getString(step++);
				model.DayofMonth = rset.getString(step++);
				model.DayOfWeek = rset.getString(step++);
				model.DepTime = rset.getString(step++);
				model.CRSDepTime = rset.getString(step++);
				model.ArrTime = rset.getString(step++);
				model.CRSArrTime = rset.getString(step++);
				model.UniqueCarrier = rset.getString(step++);
				model.FlightNum = rset.getString(step++);
				model.TailNum = rset.getString(step++);
				model.ActualElapsedTime = rset.getString(step++);
				model.CRSElapsedTime = rset.getString(step++);
				model.AirTime = rset.getString(step++);
				model.ArrDelay = rset.getString(step++);
				model.DepDelay = rset.getString(step++);
				model.Origin = rset.getString(step++);
				model.Dest = rset.getString(step++);
				model.Distance = rset.getString(step++);
				model.TaxiIn = rset.getString(step++);
				model.TaxiOut = rset.getString(step++);
				model.Cancelled = rset.getString(step++);
				model.CancellationCode = rset.getString(step++);
				model.Diverted = rset.getString(step++);
				model.CarrierDelay = rset.getString(step++);
				model.WeatherDelay = rset.getString(step++);
				model.NASDelay = rset.getString(step++);
				model.SecurityDelay = rset.getString(step++);
				model.LateAircraftDelay = rset.getString(step++);	
				
				resultList.add(model);
			}
			
			logDataSourceStatistics();
			
			rset.close();
			pstmt.close();
		} catch(Exception e){
			System.err.println(e.getMessage());
			throw new RetryException("ERROR SAVING DATA IN ORACLE");
		} 
		
		return resultList;
	}

	
	public static int saveDataFromLine(String line){
		
		FlightModel model = MappingUtils.lineToModel(line);
		
		try(Connection conn = JdbcConnection.getConnection()){
			
			PreparedStatement pstmt = conn.prepareStatement(INSERT_FLIGHT);
			int step = 1;
			pstmt.setString(step, model.Year);
			pstmt.setString(step++, model.Month);
			pstmt.setString(step++, model.DayofMonth);
			pstmt.setString(step++, model.DayOfWeek);
			pstmt.setString(step++, model.DepTime);
			pstmt.setString(step++, model.CRSDepTime);
			pstmt.setString(step++, model.ArrTime);
			pstmt.setString(step++, model.CRSArrTime);
			pstmt.setString(step++, model.UniqueCarrier);
			pstmt.setString(step++, model.FlightNum);
			pstmt.setString(step++, model.TailNum);
			pstmt.setString(step++, model.ActualElapsedTime);
			pstmt.setString(step++, model.CRSElapsedTime);
			pstmt.setString(step++, model.AirTime);
			pstmt.setString(step++, model.ArrDelay);
			pstmt.setString(step++, model.DepDelay);
			pstmt.setString(step++, model.Origin);
			pstmt.setString(step++, model.Dest);
			pstmt.setString(step++, model.Distance);
			pstmt.setString(step++, model.TaxiIn);
			pstmt.setString(step++, model.TaxiOut);
			pstmt.setString(step++, model.Cancelled);
			pstmt.setString(step++, model.CancellationCode);
			pstmt.setString(step++, model.Diverted);
			pstmt.setString(step++, model.CarrierDelay);
			pstmt.setString(step++, model.WeatherDelay);
			pstmt.setString(step++, model.NASDelay);
			pstmt.setString(step++, model.SecurityDelay);
			pstmt.setString(step++, model.LateAircraftDelay);	
			pstmt.setString(step++, model.LateAircraftDelay);	

			logDataSourceStatistics();
			
			return pstmt.executeUpdate();
			
		} catch(Exception e){
			System.err.println(e.getMessage());
			throw new RetryException("ERROR SAVING DATA IN ORACLE");
		}
		
	}

	public static int saveAsJson(FlightModel model){
		
		String jsonValue = MappingUtils.modelToJson(model);
		
		try(Connection conn = JdbcConnection.getConnection()){
			
			PreparedStatement pstmt = conn.prepareStatement(INSERT_FLIGHT_JSON);
			pstmt.setString(1, jsonValue);

			logDataSourceStatistics();
			
			return pstmt.executeUpdate();
			
		} catch(Exception e){
			System.err.println(e.getMessage());
			throw new RetryException("ERROR SAVING DATA IN ORACLE");
		}
		
	}
	
	@SuppressWarnings("all")
	private static void logDataSourceStatistics(){
		if (MigrateGSApp.LOG_DS_STATISTICS && Calendar.getInstance().get(Calendar.SECOND) % 30 == 0) {
			JdbcConnection.logActiveConnections();
		}
	}
	
}

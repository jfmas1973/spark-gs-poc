package es.jfmas.tests.spark.model;

import java.io.Serializable;

public class FlightModel implements Serializable {

	/** Serial Id **/
	private static final long serialVersionUID = 4166545822485492063L;
	
	public Long FlightId;
	
	public String Year;
	public String Month;
	public String DayofMonth;
	public String DayOfWeek;
	public String DepTime;
	public String CRSDepTime;
	public String ArrTime;
	public String CRSArrTime;
	public String UniqueCarrier;
	public String FlightNum;
	public String TailNum;
	public String ActualElapsedTime;
	public String CRSElapsedTime;
	public String AirTime;
	public String ArrDelay;
	public String DepDelay;
	public String Origin;
	public String Dest;
	public String Distance;
	public String TaxiIn;
	public String TaxiOut;
	public String Cancelled;
	public String CancellationCode;
	public String Diverted;
	public String CarrierDelay;
	public String WeatherDelay;
	public String NASDelay;
	public String SecurityDelay;
	public String LateAircraftDelay;	
	
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof FlightModel && FlightId != null) {
			return FlightId.equals(((FlightModel) obj).FlightId);
		}
		return super.equals(obj);
	}

	@Override
	public int hashCode() {
		if (FlightId != null) {
			return FlightId.hashCode();
		}
		return super.hashCode();
	}
		
}

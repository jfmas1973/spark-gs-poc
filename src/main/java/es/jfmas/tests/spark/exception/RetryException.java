package es.jfmas.tests.spark.exception;

/**
 * Exception to control the points that could be retried.
 * TODO Implement this in MigrateGSApp convertoJson.
 * @author jfmas
 *
 */
public class RetryException extends RuntimeException {

	/**	Serial Id **/
	private static final long serialVersionUID = 7079909188606786993L;

	public RetryException(String message){
		super(message);
	}
	
}

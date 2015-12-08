package es.jfmas.tests.spark.exception;

/**
 * Exception to control the points that could be retried. Spark stops the process if a RuntimeException happens, but not for a checked Exception.
 * So, this is the way to implement a logic of retries in some points of code.
 * TODO Implement this in MigrateGSApp convertoJson.
 * @author jfmas
 *
 */
public class RetryCheckedException extends Exception {

	/**	Serial Id **/
	private static final long serialVersionUID = 7079909188606786993L;

	public RetryCheckedException(String message){
		super(message);
	}
	
}

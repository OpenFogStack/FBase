/**
 * 
 */
package exceptions;

/**
 * @author Dave
 *
 */
public class FBaseStorageConnectorException extends FBaseException {

	public static final String CONNECTION_FAILED = "Connection to the storage system failed";
	public static final String DATABASE_FULL = "Database has no more storage capacity left";

	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public FBaseStorageConnectorException() {
		super();
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public FBaseStorageConnectorException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public FBaseStorageConnectorException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public FBaseStorageConnectorException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public FBaseStorageConnectorException(Throwable cause) {
		super(cause);
	}

}

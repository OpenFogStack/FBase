/**
 * 
 */
package exceptions;

/**
 * @author jonathanhasenburg
 *
 */
public class FBaseRestException extends FBaseException {

	public static final String DATAIDENTIFIER_MISSING =
			"The mandatory query parameter " + "dataIdentifier is missing or not parseable";
	public static final String KEYGROUPID_MISSING =
			"The mandatory query parameter " + "keygroupID is missing or not parseable";
	public static final String CLIENTID_MISSING =
			"The mandatory query parameter " + "clientID is missing or not parseable";
	public static final String NOT_FOUND = "No data record with the given identifier was found "
			+ "or no matching keygroup exists that is needed to put/get data";
	public static final String NOT_FOUND_CONFIG = "The requested keygroup config does not exist";
	public static final String SERVER_ERROR = "Please check the server log";
	public static final String BODY_NOT_PARSEABLE = "The request body is not parseable";
	public static final String DELETION_FAILURE = "Data record could not be deleted";
	public static final String NOT_AUTHORIZED =
			"You miss authorization to access the requested data";
	public static final String DATA_DOES_NOT_MATCH =
			"The query parameter does not match the data fround in the body";

	private static final long serialVersionUID = 1L;

	private Integer httpErrorCode = null;

	/**
	 * @param message
	 */
	public FBaseRestException(String message, Integer httpErrorCode) {
		super(message);
		this.httpErrorCode = httpErrorCode;
	}

	public Integer getHttpErrorCode() {
		return this.httpErrorCode;
	}

}

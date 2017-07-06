/**
 * 
 */
package exceptions;

/**
 * @author jonathanhasenburg
 *
 */
public class FBaseRestException extends FBaseException{

	public static final String DATAIDENTIFIER_MISSING = "The mandatory query parameter dataIdentifier is missing";	
	public static final String NOT_FOUND = "No data item with the given identifier was found or the matching keygroup" +
										   " is missing";
	
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

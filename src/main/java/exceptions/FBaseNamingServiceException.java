package exceptions;

public class FBaseNamingServiceException extends FBaseException {

	public static final String NOT_REACHABLE = "Naming service does not reply to messages.";
	
	private static final long serialVersionUID = 1L;

	/**
	 * @param message
	 */
	public FBaseNamingServiceException(String message) {
		super(message);
	}
	
}

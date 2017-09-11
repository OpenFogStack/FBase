package exceptions;

public class FBaseCommunicationException extends FBaseException {

	public static final String NAMING_SERVICE_NOT_REACHABLE = "Naming service does not reply "
			+ "to messages.";
	public static final String NODE_NOT_REACHABLE = "Node does not reply "
			+ "to messages.";
	
	private static final long serialVersionUID = 1L;

	/**
	 * @param message
	 */
	public FBaseCommunicationException(String message) {
		super(message);
	}
	
}

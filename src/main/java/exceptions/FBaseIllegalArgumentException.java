package exceptions;

public class FBaseIllegalArgumentException extends FBaseException {

	private static final long serialVersionUID = 1L;

	public final static String PARAM_WAS_NULL =
			"A parameter that should not be null is null.";

	/**
	 * @param message
	 */
	public FBaseIllegalArgumentException(String message) {
		super(message);
	}

}

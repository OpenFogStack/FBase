package control;

import exceptions.FBaseIllegalArgumentException;

public class Util {

	public static void notNull(Object v) throws FBaseIllegalArgumentException {
		if (v == null) {
			throw new FBaseIllegalArgumentException(FBaseIllegalArgumentException.PARAM_WAS_NULL);
		}
	}
	
	public static void notNull(Object[] v) throws FBaseIllegalArgumentException {
		for (Object o: v) {
			notNull(o);
		}
	}
	
}

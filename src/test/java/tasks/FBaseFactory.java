package tasks;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;

public class FBaseFactory {

	private static final String BASIC_PROPERTIES_FILE = "FBaseFactory_Basic";

	public static FBase basic(int instanceNumber)
			throws FBaseStorageConnectorException, InterruptedException, ExecutionException,
			TimeoutException, FBaseCommunicationException, FBaseNamingServiceException {
		FBase fBase = new FBase(BASIC_PROPERTIES_FILE + instanceNumber + ".properties");
		fBase.startup(false);
		fBase.taskmanager.storeHistory();
		return fBase;
	}

}

package tasks;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;

public class FBaseFactory {

	private static final String BASIC_PROPERTIES_FILE = "FBaseFactory_Basic";
	private static final String NAMING_SERVICE_PROPERTIES_FILE = "FBaseFactory_NamingService";

	public static FBase basic(int instanceNumber, boolean announce, boolean backgroundTasks)
			throws FBaseStorageConnectorException, InterruptedException, ExecutionException,
			TimeoutException, FBaseCommunicationException, FBaseNamingServiceException {
		FBase fBase = new FBase(BASIC_PROPERTIES_FILE + instanceNumber + ".properties");
		fBase.startup(announce, backgroundTasks);
		fBase.taskmanager.storeHistory();
		return fBase;
	}

	public static FBase namingService(int instanceNumber, boolean announce, boolean backgroundTasks)
			throws FBaseStorageConnectorException, FBaseCommunicationException,
			FBaseNamingServiceException, InterruptedException, ExecutionException,
			TimeoutException {
		FBase fBase = new FBase(NAMING_SERVICE_PROPERTIES_FILE + instanceNumber + ".properties");
		fBase.startup(announce, backgroundTasks);
		fBase.taskmanager.storeHistory();
		return fBase;
	}

}

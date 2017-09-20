package control;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;

public class Starter {

	public static void main(String[] args)
			throws FBaseStorageConnectorException, InterruptedException, ExecutionException,
			TimeoutException, FBaseCommunicationException, FBaseNamingServiceException {
		FBase fbase = new FBase("local.properties");
		fbase.startup(true);
		fbase.fillWithData();
	}

}

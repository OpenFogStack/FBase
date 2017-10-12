package control;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;

public class Starter {

	public static void main(String[] args)
			throws FBaseStorageConnectorException, InterruptedException, ExecutionException,
			TimeoutException, FBaseCommunicationException, FBaseNamingServiceException, IOException {
		FBase fbase;
		if (args.length == 1) {
			fbase = new FBase(args[0]);
		} else {
			fbase = new FBase("local.properties");
		}
		fbase.startup(true, true); // TODO 2: parse from args
		//fbase.fillWithData();
		
		System.out.println("FBase started, press any key to stop.");
		System.in.read();
		fbase.tearDown();
	}

}

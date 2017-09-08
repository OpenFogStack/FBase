package control;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import exceptions.FBaseStorageConnectorException;

public class Starter {

	public static void main(String[] args) throws FBaseStorageConnectorException,
			InterruptedException, ExecutionException, TimeoutException {
		FBase fbase = new FBase(null);
		fbase.startup(true);
		fbase.fillWithData();
	}

}

package control;

import exceptions.FBaseStorageConnectorException;

public class Starter {
		
	public static void main(String[] args) throws FBaseStorageConnectorException {
		FBase fbase = new FBase(null);
		fbase.fillWithData();
	}

}

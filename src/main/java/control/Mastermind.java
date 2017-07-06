package control;

import crypto.CryptoProvider.EncryptionAlgorithm;
import de.hasenburg.fbase.rest.WebServer;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import storageconnector.OnHeapDBConnector;

public class Mastermind {
	
	public static OnHeapDBConnector connector;
	
	public static void main(String[] args) throws FBaseStorageConnectorException {
		connector = new OnHeapDBConnector();
		connector.initiateDatabaseConnection();
		
		KeygroupID keygroupID = new KeygroupID("smartlight", "h1", "brightness");
		connector.createKeygroup(keygroupID);
		
		KeygroupConfig config = new KeygroupConfig(new KeygroupID("smartlight", "h1", "brightness"), 
				"secret", EncryptionAlgorithm.AES);
		connector.putKeygroupConfig(config.getKeygroupID(), config);
		
		DataRecord record = new DataRecord();
		record.setDataIdentifier(new DataIdentifier(keygroupID, "M-1"));
		record.setValueWithoutKey("Test Value");
		connector.putDataRecord(record);
		
		WebServer server = new WebServer();
		server.startServer();
	}

}

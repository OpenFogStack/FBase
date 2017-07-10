package control;

import communication.Publisher;
import crypto.CryptoProvider.EncryptionAlgorithm;
import de.hasenburg.fbase.rest.WebServer;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import storageconnector.OnHeapDBConnector;
import tasks.TaskManager;

public class Mastermind {
	
	public static OnHeapDBConnector connector;
	public static Publisher publisher;
	
	public static void main(String[] args) throws FBaseStorageConnectorException {
		connector = new OnHeapDBConnector();
		connector.initiateDatabaseConnection();
		
		publisher = new Publisher("tcp://localhost", 8080, null, null);
		
		KeygroupID keygroupID = new KeygroupID("smartlight", "h1", "brightness");
		KeygroupConfig config = new KeygroupConfig(new KeygroupID("smartlight", "h1", "brightness"), 
				"secret", EncryptionAlgorithm.AES);
		TaskManager.runUpdateKeygroupConfigTask(config);
		
		DataRecord record = new DataRecord();
		record.setDataIdentifier(new DataIdentifier(keygroupID, "M-1"));
		record.setValueWithoutKey("Test Value");
		TaskManager.putDataRecordTask(record);
		
		WebServer server = new WebServer();
		server.startServer();
		
	}

}

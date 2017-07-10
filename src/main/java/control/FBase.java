package control;

import communication.Publisher;
import communication.SubscriptionRegistry;
import crypto.CryptoProvider.EncryptionAlgorithm;
import de.hasenburg.fbase.rest.WebServer;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import storageconnector.AbstractDBConnector;
import storageconnector.OnHeapDBConnector;
import tasks.TaskManager;

/**
 * Main control class of a FBase machine.
 * 
 * @author jonathanhasenburg
 *
 */
public class FBase {

	public static Configuration configuration = null;
	public static AbstractDBConnector connector = null;
	public static TaskManager taskmanager = null;
	public static Publisher publisher = null;
	public static SubscriptionRegistry subscriptionRegistry = null;
	
	public FBase(String configName) throws FBaseStorageConnectorException {
		FBase.configuration = new Configuration(configName);
		FBase.connector = new OnHeapDBConnector();
		FBase.connector.initiateDatabaseConnection();
		taskmanager = new TaskManager();
		WebServer server = new WebServer();
		server.startServer();
		publisher = new Publisher("tcp://localhost", FBase.configuration.getPublisherPort(), null, null);
		subscriptionRegistry = new SubscriptionRegistry();
		
		// fill with inital data
		fillWithData();
	}
	
	private void fillWithData() {
		KeygroupID keygroupID = new KeygroupID("smartlight", "h1", "brightness");
		KeygroupConfig config = new KeygroupConfig(new KeygroupID("smartlight", "h1", "brightness"), 
				"secret", EncryptionAlgorithm.AES);
		taskmanager.runUpdateKeygroupConfigTask(config);
		
		DataRecord record = new DataRecord();
		record.setDataIdentifier(new DataIdentifier(keygroupID, "M-1"));
		record.setValueWithoutKey("Test Value");
		taskmanager.runPutDataRecordTask(record);
	}
	
}

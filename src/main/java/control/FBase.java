package control;

import communication.Publisher;
import communication.SubscriptionRegistry;
import crypto.CryptoProvider.EncryptionAlgorithm;
import de.hasenburg.fbase.rest.WebServer;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
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

	public Configuration configuration = null;
	public AbstractDBConnector connector = null;
	public TaskManager taskmanager = null;
	public Publisher publisher = null;
	public SubscriptionRegistry subscriptionRegistry = null;
	private WebServer server = null;

	public FBase(String configName) throws FBaseStorageConnectorException {
		configuration = new Configuration(configName);
		connector = new OnHeapDBConnector();
		connector.dbConnection_initiate();
		taskmanager = new TaskManager(this);
		if (configuration.getRestPort() > 0) {
			server = new WebServer(this);
			server.startServer();
		}
		publisher = new Publisher("tcp://localhost", configuration.getPublisherPort(), null, null);
		subscriptionRegistry = new SubscriptionRegistry(this);

	}

	public void tearDown() {
		publisher.shutdown();
		server.stopServer();
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void fillWithData() {
		KeygroupID keygroupID = new KeygroupID("smartlight", "h1", "brightness");
		KeygroupConfig config = new KeygroupConfig(new KeygroupID("smartlight", "h1", "brightness"),
				"secret", EncryptionAlgorithm.AES);
		taskmanager.runUpdateKeygroupConfigTask(config);

		NodeConfig nodeConfig = new NodeConfig();
		nodeConfig.setNodeID(configuration.getNodeID());
		nodeConfig.setMessagePort(configuration.getMessagePort());
		nodeConfig.setPublisherPort(configuration.getPublisherPort());
		taskmanager.runUpdateNodeConfigTask(nodeConfig);

		DataRecord record = new DataRecord();
		record.setDataIdentifier(new DataIdentifier(keygroupID, "M-1"));
		record.setValueWithoutKey("Test Value");
		taskmanager.runPutDataRecordTask(record);
	}

}

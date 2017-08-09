package control;

import communication.Publisher;
import communication.SubscriptionRegistry;
import crypto.CryptoProvider.EncryptionAlgorithm;
import de.hasenburg.fbase.rest.WebServer;
import exceptions.FBaseStorageConnectorException;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import storageconnector.AbstractDBConnector;
import storageconnector.ConfigAccessHelper;
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
	public ConfigAccessHelper configAccessHelper = null;
	public TaskManager taskmanager = null;
	public Publisher publisher = null;
	public SubscriptionRegistry subscriptionRegistry = null;
	private WebServer server = null;

	public FBase(String configName) throws FBaseStorageConnectorException {
		configuration = new Configuration(configName);
		connector = new OnHeapDBConnector();
		configAccessHelper = new ConfigAccessHelper(this);
		connector.dbConnection_initiate();
		taskmanager = new TaskManager(this);
		if (configuration.getRestPort() > 0) {
			server = new WebServer(this);
			server.startServer();
		}
		publisher = new Publisher("tcp://localhost", configuration.getPublisherPort());
		subscriptionRegistry = new SubscriptionRegistry(this);

	}

	public void tearDown() {
		publisher.shutdown();
		if (server != null) {
			server.stopServer();
		}
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void fillWithData() throws FBaseStorageConnectorException {
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.setClientID(new ClientID("C-1"));
		connector.clientConfig_put(clientConfig.getClientID(), clientConfig);
		
		KeygroupID keygroupID = new KeygroupID("smartlight", "h1", "brightness");
		KeygroupConfig config = new KeygroupConfig(new KeygroupID("smartlight", "h1", "brightness"),
				"secret", EncryptionAlgorithm.AES);
		config.addClient(clientConfig.getClientID());
		taskmanager.runUpdateKeygroupConfigTask(config, false);

		NodeConfig nodeConfig = new NodeConfig();
		nodeConfig.setNodeID(configuration.getNodeID());
		nodeConfig.setMessagePort(configuration.getMessagePort());
		nodeConfig.setPublisherPort(configuration.getPublisherPort());
		taskmanager.runUpdateNodeConfigTask(nodeConfig);

		DataRecord record = new DataRecord();
		record.setDataIdentifier(new DataIdentifier(keygroupID, "M-1"));
		record.setValueWithoutKey("Test Value");
		taskmanager.runPutDataRecordTask(record, false);
	}

}

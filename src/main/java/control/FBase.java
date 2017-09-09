package control;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import communication.NamingServiceSender;
import communication.Publisher;
import communication.SubscriptionRegistry;
import crypto.CryptoProvider.EncryptionAlgorithm;
import de.hasenburg.fbase.rest.WebServer;
import exceptions.FBaseStorageConnectorException;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import storageconnector.AbstractDBConnector;
import storageconnector.ConfigAccessHelper;
import storageconnector.OnHeapDBConnector;
import storageconnector.S3DBConnector;
import storageconnector.AbstractDBConnector.Connector;
import tasks.TaskManager;
import tasks.UpdateNodeConfigTask.Flag;

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
	public NamingServiceSender namingServiceSender = null;
	public SubscriptionRegistry subscriptionRegistry = null;
	private WebServer server = null;

	public FBase(String configName) {
		configuration = new Configuration(configName);
		publisher = new Publisher("tcp://localhost", configuration.getPublisherPort());
	}

	public void startup(boolean registerAtNamingService) throws InterruptedException,
			ExecutionException, TimeoutException, FBaseStorageConnectorException {
		if (Connector.S3.equals(configuration.getDatabaseConnector())) {
			connector = new S3DBConnector(this);
		} else {
			connector = new OnHeapDBConnector(this);
		}
		connector.dbConnection_initiate();
		configAccessHelper = new ConfigAccessHelper(this);
		taskmanager = new TaskManager(this);
		if (configuration.getRestPort() > 0) {
			server = new WebServer(this);
			server.startServer();
		}
		namingServiceSender = new NamingServiceSender(configuration.getNamingServiceAddress(),
				configuration.getNamingServicePort(), this);
		subscriptionRegistry = new SubscriptionRegistry(this);

		taskmanager.runUpdateNodeConfigTask(null, Flag.INITIAL, registerAtNamingService).get(20,
				TimeUnit.SECONDS);

		// TODO 2: Start Background Tasks, 
		
		// TODO 2: should check own node configuration regulary to figure if removed
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

		DataRecord record = new DataRecord();
		record.setDataIdentifier(new DataIdentifier(keygroupID, "M-1"));
		record.setValueWithoutKey("Test Value");
		taskmanager.runPutDataRecordTask(record, false);
	}

}

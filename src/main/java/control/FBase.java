package control;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import communication.DirectMessageReceiver;
import communication.MessageIdEvaluator;
import communication.NamingServiceSender;
import communication.Publisher;
import communication.SubscriptionRegistry;
import crypto.CryptoProvider.EncryptionAlgorithm;
import de.hasenburg.fbase.rest.WebServer;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import storageconnector.AbstractDBConnector;
import storageconnector.AbstractDBConnector.Connector;
import storageconnector.ConfigAccessHelper;
import storageconnector.OnHeapDBConnector;
import storageconnector.S3DBConnector;
import tasks.TaskManager;

/**
 * Main control class of a FBase machine.
 * 
 * @author jonathanhasenburg
 *
 */
public class FBase {

	private static Logger logger = Logger.getLogger(FBase.class.getName());

	public Configuration configuration = null;
	public AbstractDBConnector connector = null;
	public ConfigAccessHelper configAccessHelper = null;
	public TaskManager taskmanager = null;
	public Publisher publisher = null;
	public NamingServiceSender namingServiceSender = null;
	public DirectMessageReceiver directMessageReceiver = null;
	public SubscriptionRegistry subscriptionRegistry = null;
	public MessageIdEvaluator messageIdEvaluator = null;
	private WebServer server = null;

	public FBase(String configName) {
		configuration = new Configuration(configName);
	}

	public void startup(boolean announce, boolean backgroundTasks) throws InterruptedException,
			ExecutionException, TimeoutException, FBaseStorageConnectorException,
			FBaseCommunicationException, FBaseNamingServiceException {
		if (Connector.S3.equals(configuration.getDatabaseConnector())) {
			connector =
					new S3DBConnector(configuration.getNodeID());
		} else {
			connector = new OnHeapDBConnector(configuration.getNodeID());
		}
		configuration.setMachineName(connector.dbConnection_initiate());
		configAccessHelper = new ConfigAccessHelper(this);
		taskmanager = new TaskManager(this);
		if (configuration.getRestPort() > 0) {
			server = new WebServer(this);
			server.startServer();
		}
		publisher = new Publisher("tcp://0.0.0.0", configuration.getPublisherPort());

		namingServiceSender = new NamingServiceSender(configuration.getNamingServiceAddress(),
				configuration.getNamingServicePort(), this);

		directMessageReceiver =
				new DirectMessageReceiver("tcp://0.0.0.0", configuration.getMessagePort(), this);
		directMessageReceiver.startReceiving();

		subscriptionRegistry = new SubscriptionRegistry(this);
		messageIdEvaluator = new MessageIdEvaluator(this);
		messageIdEvaluator.startup();

		// start putting heartbeats (pulse 0 = default)
		taskmanager.startBackgroundPutHeartbeatTask(0);

		// add machine to node
		if (announce) {
			announceMachineAdditionToNode();
		}

		// start other background tasks (interval 0 = default)
		if (backgroundTasks) {
			taskmanager.startBackgroundPollLatesConfigurationDataForResponsibleKeygroupsTask(0);
			taskmanager.startBackgroundCheckKeygroupConfigurationsOnUpdatesTask(0);
			taskmanager.startDetectMissingHeartbeatsTask(0, 0);
			taskmanager.startBackgroundDetectMissingResponsibility(0);
			taskmanager.startBackgroundDetectLostResponsibility(0);
		}

		Thread.sleep(50);
		logger.info("FBase started, all background tasks up and running.");
	}

	private void announceMachineAdditionToNode() throws FBaseStorageConnectorException,
			FBaseCommunicationException, FBaseNamingServiceException {

		// TODO 1: Tell a node that is already registered about addition (we need a one-to-one
		// here)

	}

	public void tearDown() {
		// TODO 1: stop all background tasks
		subscriptionRegistry.deleteAllData();
		publisher.shutdown();
		namingServiceSender.shutdown();
		messageIdEvaluator.tearDown();
		directMessageReceiver.stopReception();
		if (server != null) {
			server.stopServer();
		}
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void fillWithData() throws FBaseStorageConnectorException, InterruptedException,
			ExecutionException, TimeoutException {
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.setClientID(new ClientID("C-1"));
		clientConfig.setEncryptionAlgorithm(EncryptionAlgorithm.RSA);
		clientConfig.setPublicKey(
				"MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnvVCGNNQMcI9FCio5Hhu0JNW3kXTYj+XqmuFolQyNj+kbEZgZ718i2ujRoz5PSKU8oWSwiDKDxHmhnEAwvwX2w4E/p74SSWuv18FLcobBVad4QUbJFLCp1+JmPLJuBN/Vjvke3RYUgZcZifaH8OQDaAnJsWrNlGHRjafXwFxiwHD6KA6J8mW7y+xtcS3WDsstAwVrxZMkENnkEg3syCOKPsaUuUQgo/yE2GCaJd41v8rGee+V7ThDcVqyAJpWi/tDGclEJvH2HrPsxzUnY0cl10OAkzzZiF7lWIr/cGWpRvlzygeHqT538mLckImFRwxF4EVU/N5AoDDPhWdhNCy4QIDAQAB");
		connector.clientConfig_put(clientConfig.getClientID(), clientConfig);

		KeygroupID keygroupID = new KeygroupID("smartlight", "h1", "brightness");
		KeygroupConfig config = new KeygroupConfig(new KeygroupID("smartlight", "h1", "brightness"),
				"secret", EncryptionAlgorithm.AES);
		config.addClient(clientConfig.getClientID());
		taskmanager.runUpdateKeygroupConfigTask(config, false).get(1, TimeUnit.SECONDS);

		DataRecord record = new DataRecord();
		record.setDataIdentifier(new DataIdentifier(keygroupID, "M-1"));
		record.setValueWithoutKey("Test Value");
		taskmanager.runPutDataRecordTask(record, false);
	}

}

package control;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import communication.DirectMessageReceiver;
import communication.DirectMessageSender;
import communication.MessageIdEvaluator;
import communication.NamingServiceSender;
import communication.Publisher;
import communication.SubscriptionRegistry;
import crypto.CryptoProvider.EncryptionAlgorithm;
import de.hasenburg.fbase.rest.WebServer;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseException;
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

	private List<Future<Boolean>> backgroundTaskList = new ArrayList<>();

	public FBase(String configName) {
		configuration = new Configuration(configName);
	}

	public void startup(boolean announce, boolean backgroundTasks) throws InterruptedException,
			ExecutionException, TimeoutException, FBaseStorageConnectorException,
			FBaseCommunicationException, FBaseNamingServiceException {
		if (Connector.S3.equals(configuration.getDatabaseConnector())) {
			connector = new S3DBConnector(configuration.getNodeID());
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
		backgroundTaskList.add(taskmanager.startBackgroundPutHeartbeatTask(0));

		// add machine to node
		if (announce) {
			Thread.sleep(400); // make sure own heartbeat is in database
			announceMachineAdditionToNode();
		}

		// start other background tasks (interval 0 = default)
		if (backgroundTasks) {
			backgroundTaskList.add(taskmanager
					.startBackgroundPollLatesConfigurationDataForResponsibleKeygroupsTask(0));
			backgroundTaskList
					.add(taskmanager.startBackgroundCheckKeygroupConfigurationsOnUpdatesTask(0));
			backgroundTaskList.add(taskmanager.startDetectMissingHeartbeatsTask(0, 0));
			backgroundTaskList.add(taskmanager.startBackgroundDetectMissingResponsibility(0));
			backgroundTaskList.add(taskmanager.startBackgroundDetectLostResponsibility(0));
		}

		Thread.sleep(50);
		logger.info("FBase started, all background tasks up and running.");
	}

	private void announceMachineAdditionToNode() throws FBaseStorageConnectorException,
			FBaseCommunicationException, FBaseNamingServiceException {
		Map<String, Pair<String, Long>> heartbeats = connector.heartbeats_listAll();
		if (heartbeats.size() <= 1) {
			logger.debug("We are the first machine of the node");
			// update myself (must exist before, created by another node)
			taskmanager.runAnnounceUpdateOfOwnNodeConfigurationTask();
			// TODO 2: get all keygroups in which node is either replica/trigger node from
			// naming service (important for restart)
			return;
		}

		logger.debug("In total, the node has " + heartbeats.size() + " including myself.");
		Iterator<String> iterator = heartbeats.keySet().iterator();
		while (iterator.hasNext()) {
			String next = iterator.next();
			if (!configuration.getMachineName().equals(next)) {
				DirectMessageSender sender =
						new DirectMessageSender("tcp://" + heartbeats.get(next).getValue0(),
								configuration.getMessagePort(), this);
				try {
					sender.sendAnnounceMeRequest();
					sender.shutdown();
					break;
				} catch (FBaseException e) {
					logger.debug(
							"Machine " + next + " could not announce me, trying with another one");
				}
				sender.shutdown();
			}
			// no machine left for announcing
			if (iterator.hasNext()) {
				logger.fatal("No machine could announce me, shutting down");
			}
		}
	}

	public void tearDown() {
		logger.info("Stopping background tasks");
		for (Future<Boolean> backgroundTask : backgroundTaskList) {
			backgroundTask.cancel(true);
		}
		if (server != null) {
			server.stopServer();
		}
		subscriptionRegistry.deleteAllData();
		messageIdEvaluator.tearDown();
		publisher.shutdown();
		namingServiceSender.shutdown();
		directMessageReceiver.stopReception();
		taskmanager.tearDown();
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// System.exit(0);
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

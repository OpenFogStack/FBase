package tasks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import communication.NamingServiceSender;
import communication.Publisher;
import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseEncryptionException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.data.KeygroupID;
import model.messages.Envelope;
import model.messages.Message;

/**
 * 
 * @author jonathanhasenburg
 *
 */
public class ProcessMessageWithUnknownEncryptionTaskTest {

	private static Logger logger =
			Logger.getLogger(ProcessMessageWithUnknownEncryptionTaskTest.class.getName());

	private static FBase fbase1 = null;

	private static NodeConfig nConfig1 = null;

	private static KeygroupID keygroupID = new KeygroupID("app", "tenant", "group");
	private static KeygroupConfig kConfigOld = null;
	private static KeygroupConfig kConfigNew = null;

	Publisher otherNodePublisher = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		fbase1 = new FBase("ProcessMessageWithUnknownEncryptionTaskTest.properties");
		fbase1.startup(false);
		nConfig1 = createNodeConfig(fbase1);
		logger.debug("FBase1 ready");

		kConfigOld = new KeygroupConfig(keygroupID, "secret", EncryptionAlgorithm.AES);
		kConfigNew = new KeygroupConfig(keygroupID, "secretNew", EncryptionAlgorithm.AES);
		kConfigNew.setVersion(1);

		ReplicaNodeConfig repConfig1 = new ReplicaNodeConfig();
		repConfig1.setNodeID(nConfig1.getNodeID());
		Set<ReplicaNodeConfig> replicaNodeConfigs = new HashSet<ReplicaNodeConfig>();
		replicaNodeConfigs.add(repConfig1);
		kConfigOld.setReplicaNodes(replicaNodeConfigs);
		kConfigNew.setReplicaNodes(replicaNodeConfigs);

		fbase1.taskmanager.runUpdateKeygroupConfigTask(kConfigOld, false).get(1, TimeUnit.SECONDS);
		otherNodePublisher = new Publisher("tcp://localhost", 4320);
		Thread.sleep(400);
		fbase1.subscriptionRegistry.subscribeTo(otherNodePublisher.getAddress(),
				otherNodePublisher.getPort(), kConfigOld.getEncryptionSecret(),
				kConfigOld.getEncryptionAlgorithm(), keygroupID);
	}

	@After
	public void tearDown() throws Exception {
		fbase1.tearDown();
		fbase1 = null;
		otherNodePublisher.shutdown();
		Thread.sleep(500);
		logger.debug("\n");
	}

	private NodeConfig createNodeConfig(FBase fBase) {
		NodeConfig nConfig = new NodeConfig();
		nConfig.setNodeID(fBase.configuration.getNodeID());
		nConfig.setPublisherPort(fBase.configuration.getPublisherPort());
		nConfig.setRestPort(fBase.configuration.getRestPort());
		ArrayList<String> machines = new ArrayList<String>();
		machines.add("tcp://localhost");
		nConfig.setMachines(machines);
		return nConfig;
	}

	/**
	 * The node I subscribed to publishes an envelope encrypted with the new data
	 * 
	 * @throws InterruptedException
	 * @throws FBaseEncryptionException
	 */
	private void otherNodePublishesEnvelope()
			throws InterruptedException, FBaseEncryptionException {
		Message m = new Message();
		m.setContent(JSONable.toJSON(kConfigNew));
		logger.debug(JSONable.toJSON(m));
		Envelope e = new Envelope(keygroupID, m);
		otherNodePublisher.send(e, kConfigNew.getEncryptionSecret(),
				kConfigNew.getEncryptionAlgorithm());
		Thread.sleep(3500);
	}

	@Test
	// This test only makes sense, if the naming service is not running
	public void testCannotConnectToNamingService()
			throws InterruptedException, FBaseEncryptionException, FBaseStorageConnectorException,
			FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testCannotConnectToNamingService-------");
		fbase1.namingServiceSender.shutdown();
		fbase1.namingServiceSender = new NamingServiceSender("tcp://1.2.3.4", 1234, fbase1);
		otherNodePublishesEnvelope();
		Thread.sleep(5000);
		KeygroupConfig storedConfig = fbase1.configAccessHelper.keygroupConfig_get(keygroupID);
		logger.debug("Local config equals: " + JSONable.toJSON(storedConfig));
		assertEquals(kConfigOld, storedConfig);
		assertNotEquals(kConfigNew, storedConfig);
		logger.debug("Finished testCannotConnectToNamingService.");
	}

	// @Test
	public void testRemovedFromKeygroup()
			throws FBaseEncryptionException, InterruptedException, FBaseStorageConnectorException,
			FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testRemovedFromKeygroup-------");
		assertNotNull(fbase1.namingServiceSender.sendKeygroupConfigCreate(kConfigNew));
		// TODO T: Before we can test that, we need to add another replica node
		assertNotNull(fbase1.namingServiceSender.sendKeygroupConfigDeleteNode(
				kConfigNew.getKeygroupID(), fbase1.configuration.getNodeID()));
		otherNodePublishesEnvelope();
		KeygroupConfig storedConfig = fbase1.configAccessHelper.keygroupConfig_get(keygroupID);
		logger.debug("Local config equals: " + JSONable.toJSON(storedConfig));
		assertEquals(kConfigOld, storedConfig);
		kConfigNew.setVersion(2);
		assertNotEquals(kConfigNew, storedConfig);
		// cleanup
		assertTrue(fbase1.namingServiceSender.sendNamingServiceReset());
		logger.debug("Finished testRemovedFromKeygroup.");
	}

	@Test
	// This test requires a running naming service
	public void testSuccess()
			throws FBaseEncryptionException, InterruptedException, FBaseStorageConnectorException,
			FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testCannotConnectToNamingService-------");
		assertNotNull(fbase1.namingServiceSender.sendKeygroupConfigCreate(kConfigNew));
		otherNodePublishesEnvelope();
		KeygroupConfig storedConfig = fbase1.configAccessHelper.keygroupConfig_get(keygroupID);
		logger.debug("Local config equals: " + JSONable.toJSON(storedConfig));
		assertEquals(kConfigNew, storedConfig);
		assertNotEquals(kConfigOld, storedConfig);
		// cleanup
		assertTrue(fbase1.namingServiceSender.sendNamingServiceReset());
		logger.debug("Finished testCannotConnectToNamingService.");
	}

	@Test
	public void testConfigNotExistentAnymore()
			throws FBaseEncryptionException, InterruptedException, FBaseStorageConnectorException,
			FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testConfigNotExistentAnymore-------");
		otherNodePublishesEnvelope();
		KeygroupConfig storedConfig = fbase1.configAccessHelper.keygroupConfig_get(keygroupID);
		logger.debug("Local config equals: " + JSONable.toJSON(storedConfig));
		assertEquals(kConfigOld, storedConfig);
		assertNotEquals(kConfigNew, storedConfig);
		logger.debug("Finished testConfigNotExistentAnymore.");
	}

}

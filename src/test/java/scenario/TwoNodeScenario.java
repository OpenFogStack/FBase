package scenario;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mashape.unirest.http.exceptions.UnirestException;

import client.RecordRequest;
import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.data.MessageID;
import tasks.TaskManager.TaskName;

public class TwoNodeScenario {

	private static Logger logger = Logger.getLogger(TwoNodeScenario.class.getName());

	private static FBase fbase1 = null;
	private static FBase fbase2 = null;

	private static NodeConfig nConfig1 = null;
	private static NodeConfig nConfig2 = null;

	private static KeygroupID keygroupID = new KeygroupID("app", "tenant", "group");
	private static KeygroupConfig kConfig = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		fbase1 = new FBase("TwoNodeScenario_1.properties");
		fbase1.startup(false);
		fbase1.taskmanager.storeHistory();
		fbase2 = new FBase("TwoNodeScenario_2.properties");
		fbase2.startup(false);
		fbase2.taskmanager.storeHistory();

		nConfig1 = fbase1.configuration.buildNodeConfigBasedOnData();
		nConfig2 = fbase2.configuration.buildNodeConfigBasedOnData();

		kConfig = new KeygroupConfig(keygroupID, "secret", EncryptionAlgorithm.AES);
		kConfig.addReplicaNode(new ReplicaNodeConfig(nConfig1.getNodeID()));
		kConfig.addReplicaNode(new ReplicaNodeConfig(nConfig2.getNodeID()));

		fbase1.connector.nodeConfig_put(nConfig1.getNodeID(), nConfig1);
		fbase1.connector.nodeConfig_put(nConfig2.getNodeID(), nConfig2);
		fbase1.taskmanager.runUpdateKeygroupConfigTask(kConfig, false).get(3, TimeUnit.SECONDS);
		logger.debug("FBase1 ready");

		fbase2.connector.nodeConfig_put(nConfig1.getNodeID(), nConfig1);
		fbase2.connector.nodeConfig_put(nConfig2.getNodeID(), nConfig2);
		fbase2.taskmanager.runUpdateKeygroupConfigTask(kConfig, false).get(3, TimeUnit.SECONDS);
		logger.debug("FBase2 ready");

	}

	@After
	public void tearDown() throws Exception {
		fbase1.tearDown();
		fbase2.tearDown();
		fbase1 = null;
		fbase2 = null;
		Thread.sleep(500);
		logger.debug("\n");
	}

	@Test
	public void testDifferentConfigurations() {
		logger.debug("-------Starting testDifferentConfigurations-------");
		assertFalse(nConfig1.getPublisherPort().equals(nConfig2.getPublisherPort()));
		assertFalse(nConfig1.getRestPort().equals(nConfig2.getRestPort()));
		logger.debug("Finished testDifferentConfigurations.");
	}

	@Test
	public void testUpdateKeygroupConfig() throws InterruptedException, ExecutionException,
			TimeoutException, FBaseStorageConnectorException, FBaseCommunicationException,
			FBaseNamingServiceException {
		logger.debug("-------Starting testUpdateKeygroupConfig-------");

		// we create a new keygroup config that has the same id as kConfig1
		KeygroupConfig kConfig2 = new KeygroupConfig(kConfig.getKeygroupID(),
				kConfig.getEncryptionSecret(), kConfig.getEncryptionAlgorithm());
		kConfig2.addClient(new ClientID("Client 1"));
		kConfig2.setReplicaNodes(kConfig.getReplicaNodes());

		assertNotEquals(kConfig, kConfig2);

		kConfig2.setVersion(2);
		fbase1.taskmanager.runUpdateKeygroupConfigTask(kConfig2, true).get(2, TimeUnit.SECONDS);

		Thread.sleep(200);

		logger.debug("Checking configs");
		KeygroupConfig configAtNode1 =
				fbase1.configAccessHelper.keygroupConfig_get(kConfig2.getKeygroupID());
		KeygroupConfig configAtNode2 =
				fbase2.configAccessHelper.keygroupConfig_get(kConfig2.getKeygroupID());

		assertEquals(kConfig2, configAtNode1);
		assertEquals(kConfig2, configAtNode2);

		// lets create a new config that has the same id, but a different secret
		KeygroupConfig kConfig3 = new KeygroupConfig(kConfig.getKeygroupID(),
				"aCOMPLETELYdifferentSecret", kConfig.getEncryptionAlgorithm());
		kConfig2.addClient(new ClientID("Client 1"));
		kConfig2.setReplicaNodes(kConfig.getReplicaNodes());

		assertNotEquals(kConfig, kConfig3);

		kConfig3.setVersion(3);
		logger.debug("XXXX\nXXXX\nXXXX\nXXXX\nXXXX\nTest changed secret");
		fbase1.taskmanager.runUpdateKeygroupConfigTask(kConfig3, true).get(2, TimeUnit.SECONDS);

		Thread.sleep(4000);
		assertEquals("ProcessMessageWithUnknownEncryption should have been executed once",
				new Integer(1), fbase2.taskmanager.getHistoricTaskNumbers()
						.get(TaskName.PROCESS_MESSAGE_WITH_UNKNOWN_ENCRYPTION));

		logger.debug("Finished testUpdateKeygroupConfig.");
	}

	@Test
	public void testUpdateDataRecord() throws InterruptedException, FBaseStorageConnectorException,
			ExecutionException, TimeoutException, UnirestException {
		logger.debug("-------Starting testUpdateDataRecord-------");

		RecordRequest recordRequest = new RecordRequest("localhost", 8081);

		DataRecord record = new DataRecord();
		record.setDataIdentifier(new DataIdentifier(keygroupID, "X35"));
		record.setValueWithoutKey("Test value");

		recordRequest.putDataRecord(record);

		DataRecord recordAtNode1 = fbase1.connector.dataRecords_get(record.getDataIdentifier());
		DataRecord recordAtNode2 = fbase2.connector.dataRecords_get(record.getDataIdentifier());

		logger.debug("CHECK:" + recordAtNode1.getDataID() + " - " + recordAtNode2.getDataID());

		assertEquals(record, recordAtNode1);
		assertEquals(record, recordAtNode2);

		// delete data

		recordRequest.deleteDataRecord(record.getDataIdentifier());

		Thread.sleep(200);

		assertNull(fbase1.connector.dataRecords_get(record.getDataIdentifier()));
		assertNull(fbase2.connector.dataRecords_get(record.getDataIdentifier()));

		logger.debug("CHECK: " + fbase2.connector.dataRecords_get(record.getDataIdentifier()));

		logger.debug("Finished testUpdateDataRecord.");
	}

	/**
	 * 
	 * The idea of this test is that we let FBase1 publish a number of messages, and only
	 * afterwards start FBase2. Then, we put the first messageID from FBase1 manually in
	 * FBase2 and lets FBase1 publish one more. This should let FBase2 query all missed
	 * messages manually.
	 * 
	 * @throws InterruptedException
	 * @throws FBaseStorageConnectorException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	@Test
	public void testMessageHistory() throws InterruptedException, FBaseStorageConnectorException,
			ExecutionException, TimeoutException {
		logger.debug("-------Starting testMessageHistory-------");
		// stop fbase2 subscriptions
		fbase2.subscriptionRegistry.unsubscribeFromKeygroup(kConfig.getKeygroupID());
		logger.info("Current fBase2 subscriptions: "
				+ fbase2.subscriptionRegistry.getNumberOfActiveSubscriptions());

		DataRecord record1 = new DataRecord();
		record1.setDataIdentifier(new DataIdentifier(keygroupID, "X35-1"));
		record1.setValueWithoutKey("Test value");

		DataRecord record2 = new DataRecord();
		record2.setDataIdentifier(new DataIdentifier(keygroupID, "X39-2"));
		record2.setValueWithoutKey("Test value");

		DataRecord record3 = new DataRecord();
		record3.setDataIdentifier(new DataIdentifier(keygroupID, "X44-3"));
		record3.setValueWithoutKey("Test value");

		fbase1.taskmanager.runPutDataRecordTask(record1, true).get(2, TimeUnit.SECONDS);
		fbase1.taskmanager.runPutDataRecordTask(record2, true).get(2, TimeUnit.SECONDS);
		fbase1.taskmanager.runDeleteDataRecordTask(record1.getDataIdentifier(), true).get(2,
				TimeUnit.SECONDS);

		kConfig.setVersion(2); // increase version so we can use config again
		fbase2.taskmanager.runUpdateKeygroupConfigTask(kConfig, false).get(2, TimeUnit.SECONDS);
		fbase2.messageIdEvaluator
				.addReceivedMessageID(new MessageID(nConfig1.getNodeID(), "M1", 0));
		logger.debug("FBase2 ready");

		fbase1.taskmanager.runPutDataRecordTask(record3, true).get(2, TimeUnit.SECONDS);

		Thread.sleep(4000); // waiting for messageID evaluator

		DataRecord record1N2 = fbase2.connector.dataRecords_get(record1.getDataIdentifier());
		DataRecord record2N2 = fbase2.connector.dataRecords_get(record2.getDataIdentifier());
		DataRecord record3N2 = fbase2.connector.dataRecords_get(record3.getDataIdentifier());

		assertEquals(null, record1N2);
		assertEquals(record2, record2N2);
		assertEquals(record3, record3N2);

		// now lets test what happens if we add a messageID that was not send
		logger.debug("ADDING ANOTHER MESSAGE ID, REPLIES SHOULD BE EMPTY");
		fbase2.messageIdEvaluator
				.addReceivedMessageID(new MessageID(nConfig1.getNodeID(), "M1", 10));

		Thread.sleep(4000); // waiting for messageID evaluator

		assertTrue(fbase2.messageIdEvaluator.getMissingMessageIDs().isEmpty());

		logger.debug("Finished testMessageHistory.");
	}

}

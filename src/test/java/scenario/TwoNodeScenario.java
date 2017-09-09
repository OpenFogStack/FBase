package scenario;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import client.Client;
import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import tasks.UpdateNodeConfigTask.Flag;

public class TwoNodeScenario {

	private static Logger logger = Logger.getLogger(TwoNodeScenario.class.getName());

	private static FBase fbase1 = null;
	private static FBase fbase2 = null;
	private static Client client = null;

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
		fbase2 = new FBase("TwoNodeScenario_2.properties");
		fbase2.startup(false);

		nConfig1 = createNodeConfig(fbase1);
		nConfig2 = createNodeConfig(fbase2);

		kConfig = new KeygroupConfig(keygroupID, "secret", EncryptionAlgorithm.AES);
		ReplicaNodeConfig repConfig1 = new ReplicaNodeConfig();
		repConfig1.setNodeID(nConfig1.getNodeID());
		ReplicaNodeConfig repConfig2 = new ReplicaNodeConfig();
		repConfig2.setNodeID(nConfig2.getNodeID());
		Set<ReplicaNodeConfig> replicaNodeConfigs = new HashSet<ReplicaNodeConfig>();
		replicaNodeConfigs.add(repConfig1);
		replicaNodeConfigs.add(repConfig2);
		kConfig.setReplicaNodes(replicaNodeConfigs);
		logger.debug(kConfig.getReplicaNodes().size());

		client = new Client();
	}

	private NodeConfig createNodeConfig(FBase fbase) {
		NodeConfig nConfig = new NodeConfig();
		nConfig.setNodeID(fbase.configuration.getNodeID());
		nConfig.setPublisherPort(fbase.configuration.getPublisherPort());
		nConfig.setRestPort(fbase.configuration.getRestPort());
		ArrayList<String> machines = new ArrayList<String>();
		machines.add("tcp://localhost");
		nConfig.setMachines(machines);
		return nConfig;
	}

	@After
	public void tearDown() throws Exception {
		fbase1.tearDown();
		fbase2.tearDown();
		fbase1 = null;
		fbase2 = null;
		client = null;
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
			TimeoutException, FBaseStorageConnectorException, FBaseNamingServiceException {
		logger.debug("-------Starting testUpdateKeygroupConfig-------");
		fbase1.taskmanager.runUpdateNodeConfigTask(nConfig1, Flag.PUT, false).get(2, TimeUnit.SECONDS);
		fbase1.taskmanager.runUpdateNodeConfigTask(nConfig2, Flag.PUT, false).get(2, TimeUnit.SECONDS);
		fbase1.taskmanager.runUpdateKeygroupConfigTask(kConfig, false).get(2, TimeUnit.SECONDS);
		logger.debug("FBase1 ready");

		fbase2.taskmanager.runUpdateNodeConfigTask(nConfig1, Flag.PUT, false).get(2, TimeUnit.SECONDS);
		fbase2.taskmanager.runUpdateNodeConfigTask(nConfig2, Flag.PUT, false).get(2, TimeUnit.SECONDS);
		fbase2.taskmanager.runUpdateKeygroupConfigTask(kConfig, false).get(2, TimeUnit.SECONDS);
		logger.debug("FBase2 ready");

		KeygroupConfig kConfig2 = new KeygroupConfig(kConfig.getKeygroupID(),
				kConfig.getEncryptionSecret(), kConfig.getEncryptionAlgorithm());
		kConfig2.addClient(new ClientID("Client 1"));
		kConfig2.setReplicaNodes(kConfig.getReplicaNodes());
		
		assertNotEquals(kConfig, kConfig2);

		fbase1.taskmanager.runUpdateKeygroupConfigTask(kConfig2, true).get(2, TimeUnit.SECONDS);
		
		Thread.sleep(200);

		logger.debug("Checking configs");
		KeygroupConfig configAtNode1 =
				fbase1.configAccessHelper.keygroupConfig_get(kConfig2.getKeygroupID());
		KeygroupConfig configAtNode2 =
				fbase2.configAccessHelper.keygroupConfig_get(kConfig2.getKeygroupID());

		assertEquals(kConfig2, configAtNode1);
		assertEquals(kConfig2, configAtNode2);

		// TODO T: Include test of changed secret

		logger.debug("Finished testUpdateKeygroupConfig.");
	}

	@Test
	public void testUpdateDataRecord() throws InterruptedException, FBaseStorageConnectorException,
			ExecutionException, TimeoutException {
		logger.debug("-------Starting testUpdateDataRecord-------");
		fbase1.taskmanager.runUpdateNodeConfigTask(nConfig1, Flag.PUT, false).get(2, TimeUnit.SECONDS);
		fbase1.taskmanager.runUpdateNodeConfigTask(nConfig2, Flag.PUT, false).get(2, TimeUnit.SECONDS);
		fbase1.taskmanager.runUpdateKeygroupConfigTask(kConfig, false).get(2, TimeUnit.SECONDS);
		logger.debug("FBase1 ready");

		fbase2.taskmanager.runUpdateNodeConfigTask(nConfig1, Flag.PUT, false).get(2, TimeUnit.SECONDS);
		fbase2.taskmanager.runUpdateNodeConfigTask(nConfig2, Flag.PUT, false).get(2, TimeUnit.SECONDS);
		fbase2.taskmanager.runUpdateKeygroupConfigTask(kConfig, false).get(2, TimeUnit.SECONDS);
		logger.debug("FBase2 ready");

		DataRecord record = new DataRecord();
		record.setDataIdentifier(new DataIdentifier(keygroupID, "X35"));
		record.setValueWithoutKey("Test value");

		client.runPutRecordRequest("http://localhost", 8081, record);

		DataRecord recordAtNode1 = fbase1.connector.dataRecords_get(record.getDataIdentifier());
		DataRecord recordAtNode2 = fbase2.connector.dataRecords_get(record.getDataIdentifier());

		logger.debug("CHECK:" + recordAtNode1.getDataID() + " - " + recordAtNode2.getDataID());

		assertEquals(record, recordAtNode1);
		assertEquals(record, recordAtNode2);

		// delete data

		client.runDeleteRecordRequest("http://localhost", 8081, record.getDataIdentifier());

		Thread.sleep(200);
		
		assertNull(fbase1.connector.dataRecords_get(record.getDataIdentifier()));
		assertNull(fbase2.connector.dataRecords_get(record.getDataIdentifier()));

		logger.debug("CHECK: " + fbase2.connector.dataRecords_get(record.getDataIdentifier()));

		logger.debug("Finished testUpdateDataRecord.");
	}

}

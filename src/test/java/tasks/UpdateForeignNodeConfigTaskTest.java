package tasks;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.config.TriggerNodeConfig;
import model.data.KeygroupID;
import model.data.NodeID;
import tasks.TaskManager.TaskName;

/**
 * Tests {@link UpdateForeignNodeConfigTask}.
 * 
 * @author jonathanhasenburg
 *
 */
public class UpdateForeignNodeConfigTaskTest {

	private static Logger logger = Logger.getLogger(UpdateForeignNodeConfigTaskTest.class.getName());

	private FBase fBase = null;

	private KeygroupID keygroupID1 = null;
	private KeygroupID keygroupID2 = null;
	private KeygroupID keygroupID3 = null;
	
	private KeygroupConfig kConfig1 = null;
	private KeygroupConfig kConfig2 = null;
	private KeygroupConfig kConfig3 = null;
	
	private NodeConfig nConfig1 = null;
	private NodeConfig nConfig1Diff = null;
	private NodeConfig nConfig2 = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		fBase = FBaseFactory.basic(1, false, false);
		keygroupID1 = new KeygroupID("smartlight", "h1", "brightness");
		keygroupID2 = new KeygroupID("smartlight", "h1", "lightning");
		keygroupID3 = new KeygroupID("smartlight", "h1", "sound");

		kConfig1 = new KeygroupConfig(keygroupID1, "secret", EncryptionAlgorithm.AES);
		kConfig2 = new KeygroupConfig(keygroupID2, "secret", EncryptionAlgorithm.AES);
		kConfig3 = new KeygroupConfig(keygroupID3, "secret", EncryptionAlgorithm.AES);
		
		nConfig1 = new NodeConfig();
		nConfig1.setNodeID(new NodeID("N1"));
		nConfig1.addMachine("N1M1");
		
		nConfig1Diff = new NodeConfig();
		nConfig1Diff.setNodeID(new NodeID("N1"));
		nConfig1Diff.setDescription("NConfig1Diff");
		nConfig1Diff.addMachine("N1M1");
		
		nConfig2 = new NodeConfig();
		nConfig2.setNodeID(new NodeID("N2"));
		nConfig2.addMachine("N2M1");
		
		fBase.connector.nodeConfig_put(nConfig1.getNodeID(), nConfig1);
		fBase.connector.nodeConfig_put(nConfig2.getNodeID(), nConfig2);
		
		// node configs for keygroups
		kConfig1.addReplicaNode(new ReplicaNodeConfig(nConfig1.getNodeID()));
		kConfig1.addReplicaNode(new ReplicaNodeConfig(nConfig2.getNodeID()));

		kConfig2.addReplicaNode(new ReplicaNodeConfig(nConfig1.getNodeID()));
		kConfig2.addTriggerNode(new TriggerNodeConfig(nConfig1.getNodeID()));
				
		//put
		fBase.connector.keygroupConfig_put(keygroupID1, kConfig1);
		fBase.connector.keygroupConfig_put(keygroupID2, kConfig2);
		fBase.connector.keygroupConfig_put(keygroupID3, kConfig3);
	}

	@After
	public void tearDown() throws Exception {
		fBase.tearDown();
		Thread.sleep(500);
		logger.debug("\n");
	}

	private void validateResult(boolean expectedFuture,
			boolean actualFuture, NodeConfig expectedConfig, NodeID nodeID, int expectedNumberOfTasks)
			throws FBaseStorageConnectorException {
		assertEquals("Task returned not the expected boolean", expectedFuture, actualFuture);
		assertEquals("NodeConfig in database not as expected", expectedConfig,
				fBase.connector.nodeConfig_get(nodeID));
		assertEquals("The wrong number of UpdatedKeygroupSubscriptionsTasks has been executed",
				expectedNumberOfTasks, fBase.taskmanager.getHistoricTaskNumbers()
						.get(TaskName.UPDATE_KEYGROUP_SUBSCRIPTIONS).intValue());
	}

	/**
	 * KeygroupConfig Task should abort if version did not increase.
	 */
	@Test
	public void versionAbort() throws InterruptedException, ExecutionException, TimeoutException,
			FBaseStorageConnectorException {
		logger.debug("-------Starting versionAbort-------");
		Future<Boolean> future = fBase.taskmanager.runUpdateForeignNodeConfigTask(nConfig1Diff);
		validateResult(false, future.get(3, TimeUnit.SECONDS), nConfig1, nConfig1.getNodeID(), 0);
		logger.debug("Finished versionAbort.");
	}

	@Test
	public void storeConfig() throws FBaseStorageConnectorException, InterruptedException,
			ExecutionException, TimeoutException {
		logger.debug("-------Starting storeConfig-------");
		// prepare configs by adding machines
		nConfig1.addMachine("aflsjk");
		nConfig2.addMachine("alskjd");
		
		// test nConfig1
		Future<Boolean> future = fBase.taskmanager.runUpdateForeignNodeConfigTask(nConfig1);
		validateResult(true, future.get(3, TimeUnit.SECONDS), nConfig1, nConfig1.getNodeID(), 2);
		
		// test nConfig2
		future = fBase.taskmanager.runUpdateForeignNodeConfigTask(nConfig2);
		validateResult(true, future.get(3, TimeUnit.SECONDS), nConfig2, nConfig2.getNodeID(), 3);
		
		logger.debug("Finished storeConfig.");
	}

}

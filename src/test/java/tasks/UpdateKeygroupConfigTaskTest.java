package tasks;

import static org.junit.Assert.*;

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

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.data.KeygroupID;
import storageconnector.AbstractDBConnector;

/**
 * By using the {@link UpdateKeygroupConfigTask}, {@link UpdateKeygroupSubscriptionsTask} is
 * tested as well.
 * 
 * Note, this test does not set up only one additional fBase instance with one machine.
 * However, it is checked that the machine does not subscribe to itself.
 * 
 * TODO T: It is not tested whether the removal of subscriptions is performed if they are not
 * included in a new keygroup config
 * 
 * @author jonathanhasenburg
 *
 */
public class UpdateKeygroupConfigTaskTest {

	private static Logger logger = Logger.getLogger(UpdateKeygroupConfigTaskTest.class.getName());

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
		fbase1 = new FBase("config1.properties");
		fbase2 = new FBase("config2.properties");

		nConfig1 = createNodeConfig(fbase1);
		nConfig2 = createNodeConfig(fbase2);

		fbase1.taskmanager.runUpdateNodeConfigTask(nConfig1).get(2, TimeUnit.SECONDS);
		fbase1.taskmanager.runUpdateNodeConfigTask(nConfig2).get(2, TimeUnit.SECONDS);
		logger.debug("FBase1 ready");

		fbase2.taskmanager.runUpdateNodeConfigTask(nConfig1).get(2, TimeUnit.SECONDS);
		fbase2.taskmanager.runUpdateNodeConfigTask(nConfig2).get(2, TimeUnit.SECONDS);
		logger.debug("FBase2 ready");

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
	 * Case 1:
	 * 
	 * - {@link AbstractDBConnector#keyGroupSubscriberMachines_listAll} = null
	 * 
	 * @throws TimeoutException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void test1() throws InterruptedException, ExecutionException, TimeoutException {
		logger.debug("-------Starting test1-------");
		fbase1.taskmanager.runUpdateKeygroupConfigTask(kConfig).get(1, TimeUnit.SECONDS);
		assertEquals(1, fbase1.subscriptionRegistry.getNumberOfActiveSubscriptions());
		logger.debug("Finished test1.");
	}

	/**
	 * Case 2: - {@link AbstractDBConnector#keyGroupSubscriberMachines_listAll} != null
	 * 
	 * - I am the machine that is responsible for subscriptions
	 * 
	 * Important things to test: - Don't subscribe to machines of the own node
	 * 
	 * @throws FBaseStorageConnectorException
	 * @throws TimeoutException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void test2() throws FBaseStorageConnectorException, InterruptedException,
			ExecutionException, TimeoutException {
		logger.debug("-------Starting test2-------");
		fbase1.connector.keyGroupSubscriberMachines_put(kConfig.getKeygroupID(),
				fbase1.configuration.getMachineName());
		fbase1.taskmanager.runUpdateKeygroupConfigTask(kConfig).get(1, TimeUnit.SECONDS);
		assertEquals(1, fbase1.subscriptionRegistry.getNumberOfActiveSubscriptions());
		logger.debug("Finished test2.");
	}

	/**
	 * Case 3:
	 * 
	 * - {@link AbstractDBConnector#keyGroupSubscriberMachines_listAll} != null
	 * 
	 * - Another machine is responsible for subscriptions
	 * 
	 * @throws FBaseStorageConnectorException
	 * @throws TimeoutException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void test3() throws FBaseStorageConnectorException, InterruptedException,
			ExecutionException, TimeoutException {
		logger.debug("-------Starting test2-------");
		fbase1.connector.keyGroupSubscriberMachines_put(kConfig.getKeygroupID(),
				"Machine X35");
		fbase1.taskmanager.runUpdateKeygroupConfigTask(kConfig).get(1, TimeUnit.SECONDS);
		assertEquals(0, fbase1.subscriptionRegistry.getNumberOfActiveSubscriptions());
		logger.debug("Finished test2.");
	}

}

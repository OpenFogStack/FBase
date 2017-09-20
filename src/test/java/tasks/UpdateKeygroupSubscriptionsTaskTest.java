package tasks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.config.TriggerNodeConfig;
import model.data.KeygroupID;
import model.data.NodeID;

/**
 * Tests {@link UpdateKeygroupConfigTask}.
 * 
 * @author jonathanhasenburg
 *
 */
public class UpdateKeygroupSubscriptionsTaskTest {

	private static Logger logger =
			Logger.getLogger(UpdateKeygroupSubscriptionsTaskTest.class.getName());

	private FBase fBase = null;

	private KeygroupID keygroupID1 = null;
	private KeygroupID keygroupID2 = null;
	private KeygroupID keygroupID3 = null;

	private KeygroupConfig kConfig1 = null;
	private KeygroupConfig kConfig2 = null;
	private KeygroupConfig kConfig3 = null;

	private NodeConfig nConfig1 = null;
	private NodeConfig nConfig2 = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		fBase = FBaseFactory.basic(1);
		keygroupID1 = new KeygroupID("smartlight", "h1", "brightness");
		keygroupID2 = new KeygroupID("smartlight", "h1", "lightning");
		keygroupID3 = new KeygroupID("smartlight", "h1", "sound");

		kConfig1 = new KeygroupConfig(keygroupID1, "secret", EncryptionAlgorithm.AES);
		kConfig2 = new KeygroupConfig(keygroupID2, "secret", EncryptionAlgorithm.AES);
		kConfig3 = new KeygroupConfig(keygroupID3, "secret", EncryptionAlgorithm.AES);

		nConfig1 = new NodeConfig();
		nConfig1.setNodeID(new NodeID("TN1"));
		nConfig1.setPublisherPort(8087);
		nConfig1.addMachine("localhost");
		nConfig1.addMachine("localhost");

		nConfig2 = new NodeConfig();
		nConfig2.setNodeID(new NodeID("TN2"));
		nConfig2.setPublisherPort(8088);
		nConfig2.addMachine("localhost");
		nConfig2.addMachine("localhost");
		nConfig2.addMachine("localhost");

		fBase.connector.nodeConfig_put(nConfig1.getNodeID(), JSONable.clone(nConfig1));
		fBase.connector.nodeConfig_put(nConfig2.getNodeID(), JSONable.clone(nConfig2));

		// node configs for keygroups
		kConfig1.addReplicaNode(new ReplicaNodeConfig(nConfig1.getNodeID()));
		kConfig1.addReplicaNode(new ReplicaNodeConfig(nConfig2.getNodeID()));

		kConfig2.addReplicaNode(new ReplicaNodeConfig(nConfig1.getNodeID()));
		kConfig2.addTriggerNode(new TriggerNodeConfig(nConfig1.getNodeID()));

		kConfig3.addReplicaNode(new ReplicaNodeConfig(nConfig1.getNodeID()));
	}

	@After
	public void tearDown() throws Exception {
		fBase.tearDown();
		Thread.sleep(500);
		logger.debug("\n");
	}

	private void validateResult(int expectedNumberOfSubscriptions,
			List<KeygroupID> expectedResponsibleKeygroups) throws FBaseStorageConnectorException {
		assertEquals("The number of subscriptions is not as expected",
				expectedNumberOfSubscriptions,
				fBase.subscriptionRegistry.getNumberOfActiveSubscriptions());
		Map<KeygroupID, Pair<String, Integer>> responsibleKeygroups =
				fBase.connector.keyGroupSubscriberMachines_listAll();

		int notResponsibleFor = 0;
		for (KeygroupID kID : responsibleKeygroups.keySet()) {
			if (!responsibleKeygroups.get(kID).getValue0()
					.equals(fBase.configuration.getMachineName())) {
				notResponsibleFor++;
			}
		}

		assertEquals("The number of keygroups responsible for is not as expected",
				expectedResponsibleKeygroups.size(),
				responsibleKeygroups.size() - notResponsibleFor);
		for (KeygroupID kID : expectedResponsibleKeygroups) {
			assertTrue("Machine should be responsible for " + kID + " but is not",
					responsibleKeygroups.containsKey(kID));
		}
	}

	@Test
	public void testResponsible() throws InterruptedException, ExecutionException, TimeoutException,
			FBaseStorageConnectorException {
		logger.debug("-------Starting testResponsible-------");
		// add myself to kConfig1 and kConfig2 and kConfig3
		kConfig1.addReplicaNode(new ReplicaNodeConfig(fBase.configuration.getNodeID()));
		kConfig2.addReplicaNode(new ReplicaNodeConfig(fBase.configuration.getNodeID()));
		kConfig3.addReplicaNode(new ReplicaNodeConfig(fBase.configuration.getNodeID()));

		// make someone else responsible for kConfig1
		fBase.connector.keyGroupSubscriberMachines_put(kConfig1.getKeygroupID(), "asdlfkj");
		// make myself responsible for kConfig3
		fBase.connector.keyGroupSubscriberMachines_put(kConfig3.getKeygroupID(),
				fBase.configuration.getMachineName());

		// run task
		fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(kConfig1).get(1000, TimeUnit.SECONDS);
		logger.debug(fBase.connector.keyGroupSubscriberMachines_listAll());
		fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(kConfig2).get(3, TimeUnit.SECONDS);
		fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(kConfig3).get(3, TimeUnit.SECONDS);

		// validate
		validateResult(4, (Arrays
				.asList(new KeygroupID[] { kConfig2.getKeygroupID(), kConfig3.getKeygroupID() })));
		logger.debug("Finished testResponsible.");
	}

	@Test
	public void testApartNotApart() throws InterruptedException, ExecutionException,
			TimeoutException, FBaseStorageConnectorException {
		logger.debug("-------Starting testApartNotApart-------");
		// add myself to kConfig1 and kConfig2, but not kConfig3
		kConfig1.addReplicaNode(new ReplicaNodeConfig(fBase.configuration.getNodeID()));
		kConfig2.addReplicaNode(new ReplicaNodeConfig(fBase.configuration.getNodeID()));

		// run task
		fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(kConfig1).get(3, TimeUnit.SECONDS);
		fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(kConfig2).get(3, TimeUnit.SECONDS);
		fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(kConfig3).get(3, TimeUnit.SECONDS);

		// validate
		validateResult(7, (Arrays
				.asList(new KeygroupID[] { kConfig1.getKeygroupID(), kConfig2.getKeygroupID() })));
		logger.debug("Finished testApartNotApart.");
	}

}

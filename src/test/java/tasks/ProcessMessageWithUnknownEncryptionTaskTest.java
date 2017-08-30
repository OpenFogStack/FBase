package tasks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

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

import communication.Publisher;
import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseEncryptionException;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.data.KeygroupID;
import model.messages.Envelope;
import model.messages.Message;

/**
 * TODO NS: Add test that communicate
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

	private static String namingServicePublicKey = "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBANV8ihImY7MqlsKjWw2Te9gQaH7KunKif+LmuzzLDMy1BaECom58B1V9g1NtiH9OeZVHDHiYzGYWmFM1x4KlxrECAwEAAQ==";
	private static String namingServicePrivateKey = "MIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEA1XyKEiZjsyqWwqNbDZN72BBofsq6cqJ/4ua7PMsMzLUFoQKibnwHVX2DU22If055lUcMeJjMZhaYUzXHgqXGsQIDAQABAkEAraQLRXH2G89jKlLmB2fTDk1iQOaxufXUIQDcgDkDYyfJWCGMQew50IA2GEWRfVurz9WkjkxeP05BD4EujdhlcQIhAO6xO1X04VE2JuSlD0BDE0IxUl7F5HZI7dZ9hXW4WKj9AiEA5PdrfcVcvZUD4O4Uny0cszx4yQPoBgweCnl21Sn1bMUCIBq2DrPR0Z0q+DNCHXDNkMwphNRCRQzPoH4OUe8YkCNpAiB3c/mZaTEEG00luS/7B18Ux3TAcpBHL2Uw08PCXByVfQIhAK7E5Q13a2uu4pyTrSykjMlbQfiGgEJKju8fATIiuiCT";

	Publisher otherNodePublisher = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		fbase1 = new FBase("config1.properties");
		fbase1.startup();
		nConfig1 = createNodeConfig(fbase1);
		fbase1.taskmanager.runUpdateNodeConfigTask(nConfig1).get(2, TimeUnit.SECONDS);
		logger.debug("FBase1 ready");

		kConfigOld = new KeygroupConfig(keygroupID, "secret", EncryptionAlgorithm.AES);
		kConfigNew = new KeygroupConfig(keygroupID, "secretNew", EncryptionAlgorithm.AES);

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

	@Test
	public void testCannotConnectToNamingService()
			throws InterruptedException, FBaseEncryptionException, FBaseStorageConnectorException {
		logger.debug("-------Starting testCannotConnectToNamingService-------");
		Message m = new Message();
		m.setContent(JSONable.toJSON(kConfigNew));
		logger.debug(JSONable.toJSON(m));
		Envelope e = new Envelope(keygroupID, m);
		otherNodePublisher.send(e, kConfigNew.getEncryptionSecret(),
				kConfigNew.getEncryptionAlgorithm());
		Thread.sleep(3500);
		KeygroupConfig storedConfig = fbase1.configAccessHelper.keygroupConfig_get(keygroupID);
		assertEquals(kConfigOld, storedConfig);
		assertNotEquals(kConfigNew, storedConfig);
		logger.debug("Finished testCannotConnectToNamingService.");
	}

	@Test
	public void testRemovedFromKeygroup() {
		fail("Not yet implemented");
	}

	@Test
	public void testSuccess() {
		fail("Not yet implemented");
	}
}

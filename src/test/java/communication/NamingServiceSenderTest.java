package communication;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.ZMQ;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import model.JSONable;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.config.TriggerNodeConfig;
import model.data.ClientID;
import model.data.KeygroupID;
import model.data.NodeID;
import model.messages.Envelope;
import model.messages.Message;
import model.messages.ResponseCode;
import tasks.FBaseFactory;

public class NamingServiceSenderTest {

	private static Logger logger = Logger.getLogger(NamingServiceSenderTest.class.getName());

	private static ExecutorService executor;
	private static FBase fbase;
	private static final String ownNodeConfigJSONPath =
			"src/test/resources/NamingServiceSenderTest_NodeConfig.json";

	private static NamingServiceSender localSender = null;
	private static NamingServiceSender nsSender = null;

	private static String localAddress = "tcp://localhost";
	private static int localPort = 1234;

	Envelope e = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		executor = Executors.newCachedThreadPool();
		fbase = FBaseFactory.namingService(1, false, false);
		localSender = new NamingServiceSender(localAddress, localPort, null);
		nsSender = new NamingServiceSender(fbase.configuration.getNamingServiceAddress(),
				fbase.configuration.getNamingServicePort(), fbase);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		localSender.shutdown();
		nsSender.shutdown();
		executor.shutdownNow();
		fbase.tearDown();
	}

	@Before
	public void setUp() throws Exception {
		Message m = new Message();
		m.setTextualInfo("TestText");
		e = new Envelope(new NodeID("Node A"), m);
	}

	@After
	public void tearDown() throws Exception {
		logger.debug("Naming Sender reset: " + nsSender.sendNamingServiceReset());
		logger.debug("\n");
	}

	@Test
	public void testPollingSuccess() throws InterruptedException, ExecutionException,
			TimeoutException, FBaseCommunicationException {
		logger.debug("-------Starting testPollingSuccess-------");
		Future<?> future = executor.submit(new ReceiveHelper());
		Thread.sleep(200);
		String reply = localSender.send(e, null, null);
		future.get(5, TimeUnit.SECONDS);
		assertEquals("Success", reply);
		logger.debug("Finished testPollingSuccess.");
	}

	@Test
	public void testPollingNoResponse() {
		logger.debug("-------Starting testPollingNoResponse-------");
		try {
			@SuppressWarnings("unused")
			String reply = localSender.send(e, null, null);
		} catch (FBaseCommunicationException e) {
			logger.debug(e.getMessage());
			return;
		}
		fail("Should have catched an exception");
		logger.debug("Finished testPollingNoResponse.");
	}

	@Test
	public void testPollingRecovery() throws InterruptedException, FBaseCommunicationException,
			ExecutionException, TimeoutException {
		logger.debug("-------Starting testPollingRecovery-------");
		try {
			@SuppressWarnings("unused")
			String reply = localSender.send(e, null, null);
		} catch (FBaseCommunicationException e) {
			logger.debug(e.getMessage());
		}
		Future<?> future = executor.submit(new ReceiveHelper());
		Thread.sleep(200);
		String reply = localSender.send(e, null, null);
		future.get(5, TimeUnit.SECONDS);
		assertEquals("Success", reply);
		logger.debug("Finished testPollingRecovery.");
	}

	@Test
	public void testMalformattedURI() throws InterruptedException, FBaseCommunicationException,
			ExecutionException, TimeoutException {
		logger.debug("-------Starting testMalformattedURI-------");
		NamingServiceSender malSender = new NamingServiceSender("asdlkfasdfjk", -1, null);
		String reply = malSender.send(e, null, null);
		assertNull(reply);
		logger.debug("Finished testMalformattedURI.");
	}

	class ReceiveHelper implements Runnable {

		@Override
		public void run() {
			ZMQ.Context context = ZMQ.context(1);
			ZMQ.Socket receiver = context.socket(ZMQ.REP);
			receiver.bind(localSender.getAddress() + ":" + localSender.getPort());
			String keygroupID = receiver.recvStr();
			logger.debug("Keygroup: " + keygroupID);
			String content = receiver.recvStr();
			Message m = JSONable.fromJSON(content, Message.class);
			logger.info("Message: " + m.getTextualInfo());
			receiver.send("Success");
			logger.debug("Closing receiver");
			receiver.close();
			context.term();
		}

	}

	/*
	 * The following tests need to communicate with an actual naming service that accepts
	 * requests from this node (see config1.properties)
	 */

	/*
	 * NODE CONFIG Tests
	 */

	/**
	 * Creates a node config, either with default values or based on the values found at
	 * jsonFilePath (if not null).
	 * 
	 * @param jsonFilePath
	 * @return the config
	 */
	private NodeConfig makeNodeConfig(String jsonFilePath) {
		if (jsonFilePath == null) {
			// Set up original version of node
			NodeID id = new NodeID("test_node");
			String key1 = "my_public_key";
			EncryptionAlgorithm alg1 = EncryptionAlgorithm.AES;
			List<String> machines1 = new ArrayList<String>();
			machines1.add("m1");
			machines1.add("m2");
			machines1.add("m3");
			Integer pPort1 = 1001;
			Integer mPort1 = 2001;
			Integer rPort1 = 3001;
			String loc1 = "my_location";
			String desc1 = "my_description";

			return new NodeConfig(id, key1, alg1, machines1, pPort1, mPort1, rPort1, loc1, desc1);
		} else {
			File initialNodeFile = new File(jsonFilePath);
			String initialNodeJSON = null;

			try {
				FileReader reader = new FileReader(initialNodeFile);
				char[] chars = new char[(int) initialNodeFile.length()];
				reader.read(chars);
				initialNodeJSON = new String(chars);
				reader.close();
			} catch (FileNotFoundException e) {
				throw new IllegalArgumentException("Path " + jsonFilePath + " does not exist");
			} catch (IOException e) {
				e.printStackTrace();
			}

			return JSONable.fromJSON(initialNodeJSON, NodeConfig.class);
		}

	}

	private void createNodeConfig(NodeConfig config, boolean expected)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		boolean actual = nsSender.sendNodeConfigCreate(config);
		assertEquals("Could not create node config.", expected, actual);
	}

	private void updateNodeConfig(NodeConfig config, boolean expected)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		boolean actual = nsSender.sendNodeConfigUpdate(config);
		assertEquals("Could not update node config.", expected, actual);
	}

	private void readNodeConfig(NodeID nodeID, NodeConfig expectedConfig)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		NodeConfig actualConfig = nsSender.sendNodeConfigRead(nodeID);
		assertEquals("The naming service returned a wrong config", expectedConfig, actualConfig);
	}

	private void deleteNodeConfig(NodeID nodeID, boolean expected)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		boolean actual = nsSender.sendNodeConfigDelete(nodeID);
		assertEquals("Could not delete node config", expected, actual);
	}

	@Test
	public void testSendNodeConfigCreate() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendNodeConfigCreate-------");
		NodeConfig newConfig = makeNodeConfig(ownNodeConfigJSONPath);
		newConfig.setNodeID(new NodeID("B2"));
		createNodeConfig(newConfig, true);
		readNodeConfig(newConfig.getNodeID(), newConfig);
		logger.debug("Finished testSendNodeConfigCreate.");
	}

	@Test
	public void testSendNodeConfigUpdate() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendNodeConfigUpdate-------");
		NodeConfig configUpdate = makeNodeConfig(ownNodeConfigJSONPath);
		configUpdate.setDescription("This is a changed description");
		updateNodeConfig(configUpdate, true);
		configUpdate.setVersion(2);
		readNodeConfig(configUpdate.getNodeID(), configUpdate);
		logger.debug("Finished testSendNodeConfigUpdate.");
	}

	@Test
	public void testSendNodeConfigRead() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendNodeConfigRead-------");
		readNodeConfig(fbase.configuration.getNodeID(), makeNodeConfig(ownNodeConfigJSONPath));
		// not existent config
		try {
			nsSender.sendNodeConfigRead(new NodeID("asdlkfj"));
			fail("Should have thrwon an exception");
		} catch (Exception e) {
			assertEquals(ResponseCode.ERROR_DOESNT_EXIST.toString(), e.getMessage());

		}
		logger.debug("Finished testSendNodeConfigRead.");
	}

	@Test
	public void testSendNodeConfigDelete() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendNodeConfigDelete-------");
		deleteNodeConfig(fbase.configuration.getNodeID(), true);
		try {
			deleteNodeConfig(fbase.configuration.getNodeID(), false);
			fail("Should have thrwon an exception");
		} catch (Exception e) {
			assertEquals(ResponseCode.ERROR_TOMBSTONED.toString(), e.getMessage());
		}
		logger.debug("Finished testSendNodeConfigDelete.");
	}

	/*
	 * Client CONFIG Tests
	 */

	/**
	 * Creates a client config with default values
	 * 
	 * @return the config
	 */
	private ClientConfig makeClientConfig() {
		ClientConfig config = new ClientConfig();
		config.setClientID(new ClientID("test-client"));
		config.setEncryptionAlgorithm(EncryptionAlgorithm.RSA);
		config.setVersion(1);
		config.setPublicKey("<Put your public key here>");
		return config;
	}

	private void createClientConfig(ClientConfig config, boolean expected)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		boolean actual = nsSender.sendClientConfigCreate(config);
		assertEquals("Could not create node config.", expected, actual);
	}

	private void updateClientConfig(ClientConfig config, boolean expected)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		boolean actual = nsSender.sendClientConfigUpdate(config);
		assertEquals("Could not update node config.", expected, actual);
	}

	private void readClientConfig(ClientID clientID, ClientConfig expectedConfig)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		ClientConfig actualConfig = nsSender.sendClientConfigRead(clientID);
		assertEquals("The naming service returned a wrong config", expectedConfig, actualConfig);
	}

	private void deleteClientConfig(ClientID clientID, boolean expected)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		boolean actual = nsSender.sendClientConfigDelete(clientID);
		assertEquals("Could not delete node config", expected, actual);
	}

	@Test
	public void testSendClientConfigCreateAndRead() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendClientConfigCreateAndRead-------");
		ClientConfig newConfig = makeClientConfig();
		createClientConfig(newConfig, true);
		readClientConfig(newConfig.getClientID(), newConfig);
		logger.debug("Finished testSendClientConfigCreateAndRead.");
	}

	@Test
	public void testSendClientConfigUpdate() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendClientConfigUpdate-------");
		ClientConfig newConfig = makeClientConfig();
		createClientConfig(newConfig, true);
		ClientConfig configUpdate = makeClientConfig();
		configUpdate.setVersion(2);
		updateClientConfig(configUpdate, true);
		readClientConfig(configUpdate.getClientID(), configUpdate);
		logger.debug("Finished testSendClientConfigUpdate.");
	}

	@Test
	public void testSendClientConfigDelete() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendClientConfigDelete-------");
		ClientConfig newConfig = makeClientConfig();
		createClientConfig(newConfig, true);
		deleteClientConfig(newConfig.getClientID(), true);
		try {
			deleteClientConfig(newConfig.getClientID(), false);
			fail("Should have thrwon an exception");
		} catch (Exception e) {
			assertEquals(ResponseCode.ERROR_TOMBSTONED.toString(), e.getMessage());
		}
		logger.debug("Finished testSendClientConfigDelete.");
	}

	/*
	 * Client CONFIG Tests
	 */

	/**
	 * Creates a keygroup config with default values (which is pretty much empty)
	 * 
	 * @return the config
	 */
	private KeygroupConfig makeKeygroupConfig() {
		KeygroupConfig config = new KeygroupConfig();
		config.setKeygroupID(new KeygroupID("smartlight", "h1", "brightness"));
		config.setVersion(0);
		return config;
	}

	private KeygroupConfig createKeygroupConfig(KeygroupConfig config, int expectedVersion)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		KeygroupConfig actual = nsSender.sendKeygroupConfigCreate(config);
		assertEquals("Could not create keygroup config.", expectedVersion, actual.getVersion());
		return actual;
	}

	private void readKeygroupConfig(KeygroupID keygroupID, KeygroupConfig expectedConfig)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		KeygroupConfig actualConfig = nsSender.sendKeygroupConfigRead(keygroupID);
		logger.debug("Expected: " + JSONable.toJSON(expectedConfig));
		logger.debug("Actual: " + JSONable.toJSON(actualConfig));
		assertEquals("The naming service returned a wrong config", expectedConfig, actualConfig);
	}

	private void deleteKeygroupConfig(KeygroupID keygroupID, boolean expected)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		boolean actual = nsSender.sendKeygroupConfigDelete(keygroupID);
		assertEquals("Could not delete keygroup config", expected, actual);
	}

	@Test
	public void testSendKeygroupConfigCreateAndRead() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendKeygroupConfigCreateAndRead-------");
		KeygroupConfig newConfig = makeKeygroupConfig();
		createKeygroupConfig(newConfig, 1);
		// update keygroup config data for tests
		newConfig.setVersion(1);
		newConfig.addReplicaNode(new ReplicaNodeConfig(fbase.configuration.getNodeID()));
		readKeygroupConfig(newConfig.getKeygroupID(), newConfig);
		logger.debug("Finished testSendKeygroupConfigCreateAndRead.");
	}

	@Test
	public void testSendKeygroupConfigAddAndDeleteClient() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendKeygroupConfigAddAndDeleteClient-------");
		KeygroupConfig kConfig = makeKeygroupConfig();
		createKeygroupConfig(kConfig, 1);
		ClientConfig cConfig = makeClientConfig();
		createClientConfig(cConfig, true);
		nsSender.sendKeygroupConfigAddClient(cConfig.getClientID(), kConfig.getKeygroupID());
		// update keygroup config data for tests
		kConfig.setVersion(2);
		kConfig.addReplicaNode(new ReplicaNodeConfig(fbase.configuration.getNodeID()));
		kConfig.addClient(cConfig.getClientID());
		// check data
		readKeygroupConfig(kConfig.getKeygroupID(), kConfig);

		// delete
		nsSender.sendKeygroupConfigDeleteClient(cConfig.getClientID(), kConfig.getKeygroupID());
		// update keygroup config data for tests
		kConfig.setVersion(3);
		kConfig.removeClient(cConfig.getClientID());
		// check data
		readKeygroupConfig(kConfig.getKeygroupID(), kConfig);
		logger.debug("Finished testSendKeygroupConfigAddAndDeleteClient.");
	}

	@Test
	public void testSendKeygroupConfigAddAndDeleteTriggerNode() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendKeygroupConfigAddAndDeleteTriggerNode-------");
		// create trigger node and trigger node config
		NodeConfig triggerNode = makeNodeConfig(null);
		triggerNode.setNodeID(new NodeID("triggerNode"));
		createNodeConfig(triggerNode, true);
		TriggerNodeConfig tnConfig = new TriggerNodeConfig(triggerNode.getNodeID());

		// create keygroup
		KeygroupConfig kConfig = makeKeygroupConfig();
		createKeygroupConfig(kConfig, 1);

		// update keygroup
		nsSender.sendKeygroupConfigAddTriggerNode(tnConfig, kConfig.getKeygroupID());

		// update keygroup config data for tests
		kConfig.setVersion(2);
		kConfig.addReplicaNode(new ReplicaNodeConfig(fbase.configuration.getNodeID()));
		kConfig.addTriggerNode(tnConfig);
		// check data
		readKeygroupConfig(kConfig.getKeygroupID(), kConfig);

		// delete
		nsSender.sendKeygroupConfigDeleteNode(kConfig.getKeygroupID(), triggerNode.getNodeID());
		// update keygroup config data for tests
		kConfig.setVersion(3);
		kConfig.removeTriggerNode(triggerNode.getNodeID());
		// check data
		readKeygroupConfig(kConfig.getKeygroupID(), kConfig);
		logger.debug("Finished testSendKeygroupConfigAddAndDeleteTriggerNode.");
	}

	@Test
	public void testSendKeygroupConfigAddReplicaNode() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendKeygroupConfigAddReplicaNode-------");
		// create replica node and replica node config
		NodeConfig replicaNode = makeNodeConfig(null);
		replicaNode.setNodeID(new NodeID("replicaNode"));
		createNodeConfig(replicaNode, true);
		ReplicaNodeConfig rnConfig = new ReplicaNodeConfig(replicaNode.getNodeID(), 20000);

		// create keygroup
		KeygroupConfig kConfig = makeKeygroupConfig();
		createKeygroupConfig(kConfig, 1);

		// update keygroup
		nsSender.sendKeygroupConfigAddReplicaNode(rnConfig, kConfig.getKeygroupID());

		// update keygroup config data for tests
		kConfig.setVersion(2);
		kConfig.addReplicaNode(new ReplicaNodeConfig(fbase.configuration.getNodeID()));
		kConfig.addReplicaNode(rnConfig);
		// check data
		readKeygroupConfig(kConfig.getKeygroupID(), kConfig);
		logger.debug("Finished testSendKeygroupConfigAddReplicaNode.");
	}

	@Test
	public void testSendKeygroupConfigUpdateCrypto()
			throws FBaseCommunicationException, InterruptedException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendKeygroupConfigUpdateCrypto-------");
		// create keygroup
		KeygroupConfig kConfig = makeKeygroupConfig();
		createKeygroupConfig(kConfig, 1);
		// update crypto
		nsSender.sendKeygroupConfigUpdateCrypto("theNewMasterKey", EncryptionAlgorithm.AES,
				kConfig.getKeygroupID());
		// update keygroup config data for tests
		kConfig.setVersion(2);
		kConfig.addReplicaNode(new ReplicaNodeConfig(fbase.configuration.getNodeID()));
		kConfig.setEncryptionSecret("theNewMasterKey");
		kConfig.setEncryptionAlgorithm(EncryptionAlgorithm.AES);
		// check data
		readKeygroupConfig(kConfig.getKeygroupID(), kConfig);
		logger.debug("Finished testSendKeygroupConfigUpdateCrypto.");
	}

	@Test
	public void testSendKeygroupConfigDeleteNode() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendKeygroupConfigDeleteNode-------");
		// create keygroup
		KeygroupConfig kConfig = makeKeygroupConfig();
		createKeygroupConfig(kConfig, 1);

		// create replica node and replica node config
		NodeConfig replicaNode = makeNodeConfig(null);
		replicaNode.setNodeID(new NodeID("replicaNode"));
		createNodeConfig(replicaNode, true);
		ReplicaNodeConfig rnConfig = new ReplicaNodeConfig(replicaNode.getNodeID(), 20000);

		// update keygroup
		nsSender.sendKeygroupConfigAddReplicaNode(rnConfig, kConfig.getKeygroupID());

		// update keygroup config data for tests
		kConfig.setVersion(3);
		kConfig.addReplicaNode(rnConfig);

		// delete myself
		assertEquals(kConfig, nsSender.sendKeygroupConfigDeleteNode(kConfig.getKeygroupID(),
				fbase.configuration.getNodeID()));

		// check data
		readKeygroupConfig(kConfig.getKeygroupID(), kConfig);

		logger.debug("Finished testSendKeygroupConfigDeleteNode.");
	}

	@Test
	public void testSendKeygroupConfigDelete() throws FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting testSendKeygroupConfigDeleteNodeAndDelete-------");
		// create keygroup
		KeygroupConfig kConfig = makeKeygroupConfig();
		createKeygroupConfig(kConfig, 1);
		deleteKeygroupConfig(kConfig.getKeygroupID(), true);
		try {
			deleteKeygroupConfig(kConfig.getKeygroupID(), false);
			fail("Should have thrwon an exception");
		} catch (Exception e) {
			assertEquals(ResponseCode.ERROR_TOMBSTONED.toString(), e.getMessage());
		}
		logger.debug("Finished testSendKeygroupConfigDeleteNodeAndDelete.");
	}

}

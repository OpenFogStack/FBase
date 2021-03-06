package storageconnector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.junit.*;

import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseException;
import exceptions.FBaseStorageConnectorException;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.data.MessageID;
import model.data.NodeID;

@Ignore // remove when credentials are set
public class S3DBConnectorTest {

	private static final Logger logger = Logger.getLogger(S3DBConnectorTest.class);

	private static S3DBConnector connector;
	private KeygroupID keygroupID1;
	private KeygroupID keygroupID2;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {

	}

	@Before
	public void setUp() throws Exception {
		connector =
				new S3DBConnector(new NodeID("N1"), "de.hasenburg.fbase.s3dbconnector-testbucket");
		logger.debug("Machine name: " + connector.dbConnection_initiate());
		keygroupID1 = new KeygroupID("smartlight", "h1", "lightning");
		keygroupID2 = new KeygroupID("smartlight", "h1", "brightness");
	}

	@After
	public void tearDown() throws Exception {
		connector.deleteBuckets();
		connector.dbConnection_close();
		logger.debug("");
	}

	/*
	 * Data Records
	 */

	private List<DataRecord> generateDataRecords(int count, KeygroupID keygroupID) {
		ArrayList<DataRecord> records = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			HashMap<String, String> values = new HashMap<>();
			values.put("v1", Integer.toString(0 + (int) (Math.random() * 1000)));
			values.put("v2", Integer.toString(0 + (int) (Math.random() * 1000)));
			DataRecord record =
					new DataRecord(new DataIdentifier(keygroupID, Integer.toString(i + 1)), values);
			records.add(record);
		}
		return records;
	}

	@Test
	public void testDataRecords() throws FBaseStorageConnectorException {
		logger.debug("-------Starting testDataRecords-------");
		// generate data
		List<DataRecord> exampleRecordsKeygroup1 = generateDataRecords(5, keygroupID1);
		List<DataRecord> exampleRecordsKeygroup2 = generateDataRecords(4, keygroupID2);

		assertTrue(connector.keygroup_create(keygroupID1));
		connector.dataRecords_put(exampleRecordsKeygroup1.get(4));
		try {
			connector.dataRecords_put(exampleRecordsKeygroup2.get(0));
			fail("Should have thrown an exception");
		} catch (Exception e) {
			// we expect an exception
		}
		assertTrue(connector.keygroup_create(keygroupID2));

		// put remaining records
		for (int i = 0; i < 4; i++) {
			connector.dataRecords_put(exampleRecordsKeygroup1.get(i));
			connector.dataRecords_put(exampleRecordsKeygroup2.get(i));
		}

		// test list
		assertEquals(5, connector.dataRecords_list(keygroupID1).size());
		assertEquals(4, connector.dataRecords_list(keygroupID2).size());

		// test get
		assertEquals(exampleRecordsKeygroup1.get(3),
				connector.dataRecords_get(exampleRecordsKeygroup1.get(3).getDataIdentifier()));
		assertEquals(exampleRecordsKeygroup2.get(1),
				connector.dataRecords_get(exampleRecordsKeygroup2.get(1).getDataIdentifier()));

		// test delete
		assertTrue(
				connector.dataRecords_delete(exampleRecordsKeygroup1.get(3).getDataIdentifier()));
		assertNull(connector.dataRecords_get(exampleRecordsKeygroup1.get(3).getDataIdentifier()));

		assertEquals(4, connector.dataRecords_list(keygroupID1).size());

		logger.debug("Finished testDataRecords.");
	}

	@Test
	public void testKeygroupConfig() throws FBaseStorageConnectorException {
		logger.debug("-------Starting testKeygroupConfig-------");
		// generate data
		KeygroupConfig config1 = new KeygroupConfig(keygroupID1, "Secret", EncryptionAlgorithm.AES);
		KeygroupConfig config2 =
				new KeygroupConfig(keygroupID2, "secret 2", EncryptionAlgorithm.AES);

		// store data
		connector.keygroupConfig_put(keygroupID1, config1);
		connector.keygroupConfig_put(keygroupID2, config2);

		// check data
		assertEquals(config1, connector.keygroupConfig_get(keygroupID1));
		assertEquals(config2, connector.keygroupConfig_get(keygroupID2));

		// test list
		List<KeygroupID> keygroupConfig_list = connector.keygroupConfig_list();
		assertEquals(2, keygroupConfig_list.size());
		assertTrue(keygroupConfig_list.contains(config1.getKeygroupID()));
		assertTrue(keygroupConfig_list.contains(config2.getKeygroupID()));

		logger.debug("Finished testKeygroupConfig.");
	}

	@Test
	public void testNodeConfig() throws FBaseStorageConnectorException {
		logger.debug("-------Starting testNodeConfig-------");
		// generate data
		NodeConfig config1 = new NodeConfig();
		config1.setNodeID(new NodeID("N1"));
		NodeConfig config2 = new NodeConfig();
		config2.setNodeID(new NodeID("N2"));

		// store data
		connector.nodeConfig_put(config1.getNodeID(), config1);
		connector.nodeConfig_put(config2.getNodeID(), config2);

		// check data
		assertEquals(config1, connector.nodeConfig_get(config1.getNodeID()));
		assertEquals(config2, connector.nodeConfig_get(config2.getNodeID()));

		// test list
		List<NodeID> nodeConfig_list = connector.nodeConfig_list();
		assertEquals(2, nodeConfig_list.size());
		assertTrue(nodeConfig_list.contains(config1.getNodeID()));
		assertTrue(nodeConfig_list.contains(config2.getNodeID()));

		logger.debug("Finished testNodeConfig.");
	}

	@Test
	public void testClientConfig() throws FBaseStorageConnectorException {
		logger.debug("-------Starting testClientConfig-------");
		// generate data
		ClientConfig config1 = new ClientConfig();
		config1.setClientID(new ClientID("C1"));
		ClientConfig config2 = new ClientConfig();
		config2.setClientID(new ClientID("C2"));

		// store data
		connector.clientConfig_put(config1.getClientID(), config1);
		connector.clientConfig_put(config2.getClientID(), config2);

		// check data
		assertEquals(config1, connector.clientConfig_get(config1.getClientID()));
		assertEquals(config2, connector.clientConfig_get(config2.getClientID()));

		// test list
		List<ClientID> clientConfig_list = connector.clientConfig_list();
		assertEquals(2, clientConfig_list.size());
		assertTrue(clientConfig_list.contains(config1.getClientID()));
		assertTrue(clientConfig_list.contains(config2.getClientID()));

		logger.debug("Finished testClientConfig.");
	}

	@Test
	public void testKeygroupSubscriberMachines() throws FBaseStorageConnectorException {
		logger.debug("-------Starting testKeygroupSubscriberMachines-------");

		// store data
		assertEquals(new Integer(1),
				connector.keyGroupSubscriberMachines_put(keygroupID1, "Machine-1"));
		assertEquals(new Integer(2),
				connector.keyGroupSubscriberMachines_put(keygroupID1, "Machine-1"));
		assertEquals(new Integer(1),
				connector.keyGroupSubscriberMachines_put(keygroupID2, "Machine-2"));

		// check map
		Map<KeygroupID, Pair<String, Integer>> map = connector.keyGroupSubscriberMachines_listAll();
		assertEquals(2, map.keySet().size());
		assertEquals("Machine-1", map.get(keygroupID1).getValue0());
		assertEquals(new Integer(2), map.get(keygroupID1).getValue1());
		assertEquals("Machine-2", map.get(keygroupID2).getValue0());
		assertEquals(new Integer(1), map.get(keygroupID2).getValue1());

		// delete one object
		connector.keyGroupSubscriberMachines_remove(keygroupID1);
		map = connector.keyGroupSubscriberMachines_listAll();
		assertEquals(1, map.keySet().size());

		logger.debug("Finished testKeygroupSubscriberMachines.");
	}

	@Test
	public void testHeartbeats() throws FBaseStorageConnectorException, InterruptedException {
		logger.debug("-------Starting testHeartbeats-------");

		// store data
		Long time = System.currentTimeMillis();
		connector.heartbeats_update("M1", "addressM1");
		connector.heartbeats_update("M2", "addressM2");

		// check map
		Map<String, Pair<String, Long>> heartbeats = connector.heartbeats_listAll();
		assertEquals(2, heartbeats.keySet().size());
		assertTrue(heartbeats.get("M1").getValue1() >= time
				&& heartbeats.get("M1").getValue1() < (time + 1000));
		assertTrue(heartbeats.get("M2").getValue1() >= time
				&& heartbeats.get("M2").getValue1() < (time + 1000));
		
		assertEquals("addressM1", heartbeats.get("M1").getValue0());
		assertEquals("addressM2", heartbeats.get("M2").getValue0());

		Thread.sleep(1000);
		connector.heartbeats_update("M1", "addressMX");
		heartbeats = connector.heartbeats_listAll();
		assertTrue(heartbeats.get("M1").getValue1() >= time + 1000
				&& heartbeats.get("M1").getValue1() < (time + 2000));
		
		assertEquals("addressMX", heartbeats.get("M1").getValue0());

		logger.debug("Finished testHeartbeats.");
	}

	@Test
	public void testMessageHistory() throws InterruptedException, FBaseException {
		logger.debug("-------Starting testMessageHistory-------");		
		connector.setNodeIDAndMachineName(new NodeID("N1"), "M1");

		MessageID mID1 = new MessageID();
		mID1.setMessageIDString("N1/M1/1");
		MessageID mID2 = new MessageID();
		mID2.setMessageIDString("N1/M1/2");
		MessageID mID3 = new MessageID();
		mID3.setMessageIDString("N2/M1/3");
		MessageID mID4 = new MessageID();
		mID4.setMessageIDString("N1/M2/5");
		DataIdentifier relatedData1 = new DataIdentifier("a", "t", "g", "data");
		DataIdentifier relatedData2 = new DataIdentifier("a", "i", "g", "datas");

		// store data
		connector.messageHistory_put(mID1, relatedData1);
		connector.messageHistory_put(mID2, relatedData1);
		connector.messageHistory_put(mID3, relatedData2);
		connector.messageHistory_put(mID4, relatedData2);

		// check data
		assertEquals(relatedData1, connector.messageHistory_get(mID1));
		assertEquals(relatedData1, connector.messageHistory_get(mID2));
		assertEquals(relatedData2, connector.messageHistory_get(mID3));
		assertEquals(relatedData2, connector.messageHistory_get(mID4));

		// checkNextIDs
		assertEquals("N1/M1/3", connector.messageHistory_getNextMessageID().getMessageIDString());
		connector.setNodeIDAndMachineName(new NodeID("N2"), "M1");
		assertEquals("N2/M1/4", connector.messageHistory_getNextMessageID().getMessageIDString());
		connector.setNodeIDAndMachineName(new NodeID("N1"), "M2");
		assertEquals("N1/M2/6", connector.messageHistory_getNextMessageID().getMessageIDString());

		logger.debug("Finished testMessageHistory.");
	}

}

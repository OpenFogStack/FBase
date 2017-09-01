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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseStorageConnectorException;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.data.NodeID;

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
		connector = new S3DBConnector("de.hasenburg.fbase.s3dbconnector-testbucket");
		connector.dbConnection_initiate();
		keygroupID1 = new KeygroupID("smartlight", "h1", "lightning");
		keygroupID2 = new KeygroupID("smartlight", "h1", "brightness");
	}

	@After
	public void tearDown() throws Exception {
		connector.deleteBucket();
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

	// TODO T: Add tests for hearbeats

}

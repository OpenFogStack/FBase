package storageconnector;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import exceptions.FBaseStorageConnectorException;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;

public class S3DBConnectorTest {

	private static final Logger logger = Logger.getLogger(S3DBConnectorTest.class);

	private S3DBConnector connector;
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
		connector = new S3DBConnector();
		connector.dbConnection_initiate();
		keygroupID1 = new KeygroupID("smartlight", "h1", "lightning");
		keygroupID2 = new KeygroupID("smartlight", "h1", "brightness");
	}

	@After
	public void tearDown() throws Exception {
		// connector.deleteBucket();
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
			DataRecord record = new DataRecord(
					new DataIdentifier(keygroupID, Integer.toString(i + 1)), values);
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

}

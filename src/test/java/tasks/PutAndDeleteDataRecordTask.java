package tasks;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.data.MessageID;
import scenario.TwoNodeScenario;

/**
 * Test {@link PutDataRecordTask} and {@link DeleteDataRecordTask}.
 * 
 * Does not setup a second node to see whether publishing works, because this is already
 * tested in {@link TwoNodeScenario#testUpdateDataRecord()}
 * 
 * This test is especially important for the message history testing.
 * 
 * @author jonathanhasenburg
 *
 */
public class PutAndDeleteDataRecordTask {

	FBase fBase = null;
	private static KeygroupID keygroupID = new KeygroupID("app", "tenant", "group");

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		fBase = new FBase("PutAndDeleteDataRecordTask_Config.properties");
		fBase.startup(false);
		KeygroupConfig keygroupConfig =
				new KeygroupConfig(keygroupID, "testscret", EncryptionAlgorithm.AES);
		fBase.taskmanager.runUpdateKeygroupConfigTask(keygroupConfig, false).get(2,
				TimeUnit.SECONDS);
	}

	@After
	public void tearDown() throws Exception {
		fBase.tearDown();
	}

	@Test
	public void test() throws FBaseStorageConnectorException, InterruptedException,
			ExecutionException, TimeoutException {
		DataRecord record = new DataRecord();
		record.setDataIdentifier(new DataIdentifier(keygroupID, "X35"));
		record.setValueWithoutKey("Test value");

		MessageID mID = new MessageID(fBase.configuration.getNodeID(),
				fBase.configuration.getMachineName(), 1);
		fBase.taskmanager.runPutDataRecordTask(record, true).get(2, TimeUnit.SECONDS);
		assertEquals(record.getDataIdentifier(), fBase.connector.messageHistory_get(mID));
		assertEquals(record,
				fBase.connector.dataRecords_get(fBase.connector.messageHistory_get(mID)));

		mID = new MessageID(fBase.configuration.getNodeID(), fBase.configuration.getMachineName(),
				2);
		fBase.taskmanager.runDeleteDataRecordTask(record.getDataIdentifier(), true).get(2,
				TimeUnit.SECONDS);
		assertEquals(record.getDataIdentifier(), fBase.connector.messageHistory_get(mID));
		assertEquals(null,
				fBase.connector.dataRecords_get(fBase.connector.messageHistory_get(mID)));

	}

}

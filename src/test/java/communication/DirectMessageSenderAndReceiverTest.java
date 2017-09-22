package communication;

import static org.junit.Assert.assertEquals;

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
import exceptions.FBaseCommunicationException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.data.MessageID;

/**
 * Test for {@link DirectMessageSender} and {@link DirectMessageReceiver}.
 * 
 * @author jonathanhasenburg
 *
 */
public class DirectMessageSenderAndReceiverTest {

	private static Logger logger = Logger.getLogger(DirectMessageSenderAndReceiverTest.class.getName());

	FBase fBase = null;
	DirectMessageSender directMessageSender = null;
	private static KeygroupID keygroupID = new KeygroupID("smartlight", "h1", "brightness");
	NodeConfig myNode = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		fBase = new FBase("MessageSenderAndReceiverTest_Config.properties");
		fBase.startup(false);
		myNode = fBase.configuration.buildNodeConfigBasedOnData();
		// put my own configuration in database
		fBase.connector.nodeConfig_put(myNode.getNodeID(), myNode);
		
		directMessageSender = new DirectMessageSender(myNode, fBase);
		KeygroupConfig keygroupConfig =
				new KeygroupConfig(keygroupID, "testscret", EncryptionAlgorithm.AES);
		fBase.taskmanager.runUpdateKeygroupConfigTask(keygroupConfig, false).get(2,
				TimeUnit.SECONDS);
	}

	@After
	public void tearDown() throws Exception {
		fBase.tearDown();
		fBase = null;
		directMessageSender.shutdown();
		Thread.sleep(500);
		logger.debug("\n");
	}

	@Test
	public void test() throws InterruptedException, ExecutionException, TimeoutException,
			FBaseCommunicationException {
		logger.debug("-------Starting test-------");
		DataRecord record = new DataRecord();
		record.setDataIdentifier(new DataIdentifier(keygroupID, "X35"));
		record.setValueWithoutKey("Test value");

		MessageID mID = new MessageID(fBase.configuration.getNodeID(),
				fBase.configuration.getMachineName(), 1);

		fBase.taskmanager.runPutDataRecordTask(record, true).get(2, TimeUnit.SECONDS);

		// lets test the speed a little bit
		long n = 10;
		long startTime = System.currentTimeMillis();

		for (int i = 0; i < n; i++) {
			DataRecord recordReceived = directMessageSender.sendGetDataRecord(mID).getDataRecord();
			assertEquals(record, recordReceived);
		}

		long endTime = System.currentTimeMillis();
		logger.debug("Successfully send and received " + n + " messages in " + (endTime - startTime)
				+ " milliseconds.");

		logger.debug("Finished test.");

	}

}

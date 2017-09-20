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

import communication.Subscriber;
import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.KeygroupID;
import tasks.TaskManager.TaskName;

/**
 * Tests {@link UpdateKeygroupConfigTask}.
 * 
 * @author jonathanhasenburg
 *
 */
public class UpdateKeygroupConfigTaskTest {

	private static Logger logger = Logger.getLogger(UpdateKeygroupConfigTaskTest.class.getName());

	private FBase fBase = null;

	private KeygroupID keygroupID = null;
	private KeygroupConfig kConfigV1 = null;
	private KeygroupConfig kConfigV2 = null;

	private Subscriber subscriber = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		fBase = FBaseFactory.basic(1);
		keygroupID = new KeygroupID("smartlight", "h1", "brightness");

		kConfigV1 = new KeygroupConfig(keygroupID, "secret", EncryptionAlgorithm.AES);
		kConfigV1.setVersion(1);
		kConfigV2 = new KeygroupConfig(keygroupID, "secret", EncryptionAlgorithm.AES);
		kConfigV2.setVersion(2);

		subscriber = new Subscriber("tcp://localhost", fBase.configuration.getPublisherPort(),
				kConfigV1.getEncryptionSecret(), kConfigV1.getEncryptionAlgorithm(), null);
		subscriber.startReceiving();

		fBase.connector.keygroupConfig_put(keygroupID, kConfigV1);
	}

	@After
	public void tearDown() throws Exception {
		fBase.tearDown();
		subscriber.stopReception();
		Thread.sleep(500);
		logger.debug("\n");
	}

	private void validateResult(int expectedReceivedMessages, boolean expectedFuture,
			boolean actualFuture, KeygroupConfig expectedConfig, int expectedNumberOfTasks)
			throws FBaseStorageConnectorException {
		assertEquals("Task returned not the expected boolean", expectedFuture, actualFuture);
		assertEquals("Subscriber received a wrong number of messages", expectedReceivedMessages,
				subscriber.getNumberOfReceivedMessages());
		assertEquals("KeygroupConfig in database not as expected", expectedConfig,
				fBase.connector.keygroupConfig_get(keygroupID));
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
		Future<Boolean> future = fBase.taskmanager.runUpdateKeygroupConfigTask(kConfigV1, true);
		Thread.sleep(1000); // wait for subscriber
		validateResult(0, false, future.get(3, TimeUnit.SECONDS), kConfigV1, 0);

		kConfigV1.setVersion(0);
		future = fBase.taskmanager.runUpdateKeygroupConfigTask(kConfigV1, true);
		Thread.sleep(1000); // wait for subscriber
		validateResult(0, false, future.get(3, TimeUnit.SECONDS), kConfigV1, 0);
		logger.debug("Finished versionAbort.");
	}

	@Test
	public void storeConfig() throws FBaseStorageConnectorException, InterruptedException,
			ExecutionException, TimeoutException {
		logger.debug("-------Starting storeConfig-------");
		Future<Boolean> future = fBase.taskmanager.runUpdateKeygroupConfigTask(kConfigV2, false);
		Thread.sleep(1000); // wait for subscriber
		validateResult(0, true, future.get(3, TimeUnit.SECONDS), kConfigV2, 1);
		logger.debug("Finished storeConfig.");
	}

	@Test
	public void storeConfigAndPublish() throws FBaseStorageConnectorException, InterruptedException,
			ExecutionException, TimeoutException {
		logger.debug("-------Starting storeConfigAndPublish-------");
		Future<Boolean> future = fBase.taskmanager.runUpdateKeygroupConfigTask(kConfigV2, true);
		Thread.sleep(1000); // wait for subscriber
		validateResult(1, true, future.get(3, TimeUnit.SECONDS), kConfigV2, 1);
		logger.debug("Finished storeConfigAndPublish.");
	}

}

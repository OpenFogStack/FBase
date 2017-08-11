package communication;

import static org.junit.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import crypto.CryptoProvider.EncryptionAlgorithm;
import model.JSONable;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.messages.Envelope;
import model.messages.Message;

public class SubscriptionRegistryTest {

	private static Logger logger = Logger.getLogger(SubscriptionRegistryTest.class.getName());

	private SubscriptionRegistry subscriptionRegistry = null;
	private KeygroupID keygroupID1 = new KeygroupID("app", "tenant", "group1");

	@Before
	public void setUp() throws Exception {
		subscriptionRegistry = new SubscriptionRegistry(null);
	}

	@After
	public void tearDown() throws Exception {
		subscriptionRegistry.deleteAllData();
		logger.debug("\n");
	}

	@Test
	public void testSubscribe() throws InterruptedException {
		logger.debug("-------Starting testSubscribe-------");
		Subscriber s = subscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret",
				EncryptionAlgorithm.AES, keygroupID1);
		assertEquals(1, subscriptionRegistry.getNumberOfActiveSubscriptions());
		Publisher publisher = new Publisher("tcp://localhost", 8081);
		Thread.sleep(200);
		Message m = new Message();
		DataRecord record = new DataRecord();
		record.setValueWithoutKey("Test Value");
		m.setContent(JSONable.toJSON(record));
		Envelope e = new Envelope(keygroupID1, m);
		publisher.send(e, "secret", EncryptionAlgorithm.AES);
		Thread.sleep(200);
		assertEquals(1, s.getNumberOfReceivedMessages());
		logger.debug("Finished testSubscribe.");
	}

	@Test
	public void testUnSubscribe() {
		logger.debug("-------Starting testUnSubscribe-------");
		subscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES,
				keygroupID1);
		assertEquals(1, subscriptionRegistry.getNumberOfActiveSubscriptions());
		subscriptionRegistry.unsubscribeFromKeygroup(keygroupID1);
		assertEquals(0, subscriptionRegistry.getNumberOfActiveSubscriptions());
		logger.debug("Finished testUnSubscribe.");
	}
	
}

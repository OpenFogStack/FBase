package communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import crypto.CryptoProvider.EncryptionAlgorithm;
import model.JSONable;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.messages.datarecords.Envelope;
import model.messages.datarecords.Message;

public class SubscriptionRegistryTest {

	private static Logger logger = Logger.getLogger(SubscriptionRegistryTest.class.getName());
		
	private SubscriptionRegistry subscriptionRegistry = null;
	
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
		Subscriber s = subscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(1, subscriptionRegistry.getNumberOfActiveSubscriptions());
		Publisher publisher = new Publisher("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES);
		Thread.sleep(200);
		Message m = new Message();
		DataRecord record = new DataRecord();
		record.setValueWithoutKey("Test Value");
		m.setContent(JSONable.toJSON(record));
		Envelope e = new Envelope(new KeygroupID("app", "tenant", "group"), m);
		publisher.send(e);
		Thread.sleep(200);
		assertEquals(1, s.getNumberOfReceivedMessages());
		logger.debug("Finished testSubscribe.");
	}
	
	@Test
	public void testUnSubscribe() {
		logger.debug("-------Starting testUnSubscribe-------");
		subscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(1, subscriptionRegistry.getNumberOfActiveSubscriptions());
		subscriptionRegistry.unsubscribeFrom("tcp://localhost", 8081);
		assertEquals(0, subscriptionRegistry.getNumberOfActiveSubscriptions());
		logger.debug("Finished testUnSubscribe.");
	}
	
	@Test
	public void testSubscribeTwoSameAddressUnsubscribe() {
		logger.debug("-------Starting testSubscribeTwoSameAddressUnsubscribe-------");
		subscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES, null);
		subscriptionRegistry.subscribeTo("tcp://localhost", 8082, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(2, subscriptionRegistry.getNumberOfActiveSubscriptions());
		subscriptionRegistry.unsubscribeFrom("tcp://localhost", 8081);
		assertEquals(1, subscriptionRegistry.getNumberOfActiveSubscriptions());
		assertEquals(1, subscriptionRegistry.getNumberOfActiveSubscriptions("tcp://localhost"));
		logger.debug("Finished testSubscribeTwoSameAddressUnsubscribe.");
	}
	
	@Test
	public void testSubscribeSamePortOneAddress() {
		logger.debug("-------Starting testSubscribeSamePortOneAddress-------");
		subscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(1, subscriptionRegistry.getNumberOfActiveSubscriptions());
		assertNull(subscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES, null));
		assertEquals(1, subscriptionRegistry.getNumberOfActiveSubscriptions());
		logger.debug("Finished testSubscribeSamePortOneAddress.");
	}
	
	@Test
	public void testSubscribeSamePortDifferentAddress() {
		logger.debug("-------Starting testSubscribeSamePortDifferentAddress-------");
		subscriptionRegistry.subscribeTo("tcp://10.0.1.1", 8081, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(1, subscriptionRegistry.getNumberOfActiveSubscriptions());
		subscriptionRegistry.subscribeTo("tcp://10.0.1.2", 8081, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(2, subscriptionRegistry.getNumberOfActiveSubscriptions());
		assertEquals(1, subscriptionRegistry.getNumberOfActiveSubscriptions("tcp://10.0.1.1"));
		assertEquals(1, subscriptionRegistry.getNumberOfActiveSubscriptions("tcp://10.0.1.2"));
		logger.debug("Finished testSubscribeSamePortDifferentAddress.");
	}
	
	@Test
	public void failUnsubscribe() {
		logger.debug("-------Starting failUnsubscribe-------");
		assertFalse(subscriptionRegistry.unsubscribeFrom("localhost", 8081));
		logger.debug("Finished failUnsubscribe.");
	}
	
}
